/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workloadmanager

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
	"github.com/volcano-sh/agentcube/pkg/common/types"
	"github.com/volcano-sh/agentcube/pkg/store"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Server is the main structure for workload manager
type Server struct {
	config             *Config
	router             *gin.Engine
	httpServer         *http.Server
	webhookServer      *http.Server // dedicated TLS server for /validate/snapstart
	k8sClient          *K8sClient
	sandboxController  *SandboxReconciler
	snapshotController *SnapshotController
	tokenCache         *TokenCache
	informers          *Informers
	storeClient        store.Store
	wg                 sync.WaitGroup
}

type Config struct {
	// Port is the port the main API server listens on.
	Port string
	// RuntimeClassName is the RuntimeClassName for sandbox pods
	RuntimeClassName string
	// EnableTLS enables HTTPS for the main API server. When disabled, the main
	// API server supports plaintext h2c/HTTP.
	EnableTLS bool
	// TLSCert is the path to the TLS certificate file for the main API server.
	TLSCert string
	// TLSKey is the path to the TLS key file for the main API server.
	TLSKey string
	// EnableAuth enable auth by service account
	EnableAuth bool
	// SandboxReadyProbeTimeout is the maximum time to wait for sandbox entrypoints
	// to start accepting connections after the sandbox is reported ready.
	SandboxReadyProbeTimeout time.Duration
	// SandboxReadyProbeInterval is the retry interval for sandbox entrypoint probes.
	SandboxReadyProbeInterval time.Duration
	// WebhookPort is the port for the dedicated admission webhook HTTPS server.
	// When non-empty, a separate TLS-only server is started for /validate/snapstart
	// without changing the main API server's TLS mode. Kubernetes admission webhooks
	// require TLS.
	WebhookPort string
	// WebhookTLSCert is the path to the TLS certificate for the webhook server.
	WebhookTLSCert string
	// WebhookTLSKey is the path to the TLS private key for the webhook server.
	WebhookTLSKey string
}

// SnapshotController returns the server's SnapshotController so callers can register it
// with a controller-runtime manager via mgr.Add(server.SnapshotController()).
func (s *Server) SnapshotController() *SnapshotController {
	return s.snapshotController
}

// NewServer creates a new API server instance
func NewServer(config *Config, sandboxController *SandboxReconciler) (*Server, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.SandboxReadyProbeTimeout <= 0 {
		config.SandboxReadyProbeTimeout = defaultSandboxReadyProbeTimeout
	}
	if config.SandboxReadyProbeInterval <= 0 {
		config.SandboxReadyProbeInterval = defaultSandboxReadyProbeInterval
	}

	// Create Kubernetes client
	k8sClient, err := NewK8sClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Initialize public key cache from Router's Secret in background
	// This will retry until successful (handles case where Router isn't ready yet)
	InitPublicKeyCache(k8sClient.clientset)

	// Create token cache (cache up to 1000 tokens, 5min TTL)
	tokenCache := NewTokenCache(1000, 5*time.Minute)

	informers := NewInformers(k8sClient)
	storeClient := store.Storage()
	snapshotCtrl := newSnapshotController(k8sClient, storeClient, informers)

	server := &Server{
		config:             config,
		k8sClient:          k8sClient,
		sandboxController:  sandboxController,
		snapshotController: snapshotCtrl,
		tokenCache:         tokenCache,
		informers:          informers,
		storeClient:        storeClient,
	}

	// Setup routes
	server.setupRoutes()

	return server, nil
}

// setupRoutes configures HTTP routes on the main h2c server.
// The /validate/snapstart webhook endpoint lives on a separate TLS server
// (see startWebhookServer) and is intentionally absent from the main router.
func (s *Server) setupRoutes() {
	s.router = gin.New()

	// Health check and metrics (no authentication required)
	s.router.GET("/health", s.handleHealth)
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// API v1 routes
	v1Group := s.router.Group("/v1")
	// Apply middleware (logging first, then auth)
	v1Group.Use(s.loggingMiddleware)
	v1Group.Use(s.authMiddleware)

	// agent runtime management endpoints
	v1Group.POST("/agent-runtime", s.handleAgentRuntimeCreate)
	v1Group.DELETE("/agent-runtime/sessions/:sessionId", s.handleDeleteSandbox)
	// code interpreter management endpoints
	v1Group.POST("/code-interpreter", s.handleCodeInterpreterCreate)
	v1Group.DELETE("/code-interpreter/sessions/:sessionId", s.handleDeleteSandbox)
}

// Start starts the API server
func (s *Server) Start(ctx context.Context) error {
	// Wire up event handlers before starting informers.
	sc := s.snapshotController

	// SnapStart events: add/update trigger reconcile; delete is handled via Finalizer.
	s.informers.SnapStartInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if u, ok := obj.(*unstructured.Unstructured); ok {
				var ss runtimev1alpha1.SnapStart
				if err := unstructuredToSnapStart(u, &ss); err == nil {
					sc.indexer.upsert(&ss)
				}
				sc.Enqueue(u.GetNamespace(), u.GetName())
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if u, ok := newObj.(*unstructured.Unstructured); ok {
				var ss runtimev1alpha1.SnapStart
				if err := unstructuredToSnapStart(u, &ss); err == nil {
					sc.indexer.upsert(&ss)
				}
				sc.Enqueue(u.GetNamespace(), u.GetName())
			}
		},
		DeleteFunc: func(obj interface{}) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				if tombstone, ok2 := obj.(cache.DeletedFinalStateUnknown); ok2 {
					u, ok = tombstone.Obj.(*unstructured.Unstructured)
				}
			}
			if !ok {
				return
			}
			var ss runtimev1alpha1.SnapStart
			if err := unstructuredToSnapStart(u, &ss); err == nil {
				sc.indexer.remove(&ss)
			}
		},
	})

	// CodeInterpreter events: spec changes may invalidate existing snapshots;
	// deletion must mark related SnapStarts as degraded.
	s.informers.CodeInterpreterInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, newObj interface{}) {
			u, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			for _, ss := range sc.indexer.getByRuntime(u.GetNamespace(), u.GetName()) {
				sc.Enqueue(ss.Namespace, ss.Name)
			}
		},
		DeleteFunc: func(obj interface{}) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				if d, ok2 := obj.(cache.DeletedFinalStateUnknown); ok2 {
					u, ok = d.Obj.(*unstructured.Unstructured)
				}
			}
			if ok {
				sc.onRuntimeDeleted(context.Background(), u.GetNamespace(), u.GetName())
			}
		},
	})

	// Node events: track eligibility changes and enqueue affected SnapStarts.
	// Node availability sync (marking Redis entries Unavailable) is done inside
	// reconcile() via syncNodeAvailability, not here — event handlers should only
	// enqueue work to avoid blocking the informer and to get error handling / backoff.
	s.informers.NodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				return
			}
			// A new eligible+Ready node may need a snapshot built for it.
			if node.Labels[types.LabelKuasarSnapstart] == "true" && isNodeReady(node) {
				sc.EnqueueAllReady()
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldNode, ok1 := oldObj.(*corev1.Node)
			newNode, ok2 := newObj.(*corev1.Node)
			if !ok1 || !ok2 {
				return
			}
			hadLabel := oldNode.Labels[types.LabelKuasarSnapstart] == "true"
			hasLabel := newNode.Labels[types.LabelKuasarSnapstart] == "true"
			// Any readiness or label change may affect snapshot availability or eligibility.
			if isNodeReady(oldNode) != isNodeReady(newNode) || hadLabel != hasLabel {
				sc.EnqueueAllReady()
			}
		},
		DeleteFunc: func(_ interface{}) {
			sc.EnqueueAllReady()
		},
	})

	// Initialize store with informer before starting server
	if err := s.informers.RunAndWaitForCacheSync(ctx); err != nil {
		return fmt.Errorf("failed to wait for caches to sync: %w", err)
	}

	if err := s.storeClient.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping store: %w", err)
	}

	klog.Info("kv store Ping check successfully")

	addr := ":" + s.config.Port

	// Create HTTP/2 server for better performance
	h2s := &http2.Server{}

	// Wrap handler with h2c for HTTP/2 cleartext support
	h2cHandler := h2c.NewHandler(s.router, h2s)

	s.httpServer = &http.Server{
		Addr:        addr,
		Handler:     h2cHandler,
		ReadTimeout: 15 * time.Second,
		IdleTimeout: 90 * time.Second, // golang http default transport's idletimeout is 90s
	}

	klog.Infof("Server listening on %s", addr)

	gc := newGarbageCollector(s.k8sClient, s.storeClient, 15*time.Second)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		gc.run(ctx.Done())
	}()

	// Start the dedicated webhook HTTPS server when configured. This satisfies
	// Kubernetes' admission webhook TLS requirement without changing the main
	// API server's existing optional TLS behavior.
	if s.config.WebhookPort != "" {
		if err := s.startWebhookServer(ctx); err != nil {
			return fmt.Errorf("start webhook server: %w", err)
		}
	}

	// Start HTTP or HTTPS server
	if s.config.EnableTLS {
		if s.config.TLSCert == "" || s.config.TLSKey == "" {
			return fmt.Errorf("TLS enabled but cert/key not provided")
		}
		return s.httpServer.ListenAndServeTLS(s.config.TLSCert, s.config.TLSKey)
	}

	return s.httpServer.ListenAndServe()
}

// startWebhookServer starts a dedicated TLS-only HTTP server for /validate/snapstart.
// It runs in a background goroutine so Start() can proceed to serve the main h2c server.
func (s *Server) startWebhookServer(ctx context.Context) error {
	if s.config.WebhookTLSCert == "" || s.config.WebhookTLSKey == "" {
		return fmt.Errorf("webhook port %s configured but WebhookTLSCert/WebhookTLSKey not set", s.config.WebhookPort)
	}
	wh := newAdmissionHandler(s.snapshotController.indexer)
	mux := http.NewServeMux()
	mux.HandleFunc("/validate/snapstart", func(w http.ResponseWriter, r *http.Request) {
		// Bridge net/http → Gin so the admission handler can reuse existing Gin logic.
		g := gin.New()
		g.POST("/validate/snapstart", wh.handleValidateSnapStart)
		g.ServeHTTP(w, r)
	})
	s.webhookServer = &http.Server{
		Addr:        ":" + s.config.WebhookPort,
		Handler:     mux,
		ReadTimeout: 15 * time.Second,
		IdleTimeout: 90 * time.Second,
	}
	klog.Infof("Webhook server (TLS) listening on :%s", s.config.WebhookPort)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.webhookServer.ListenAndServeTLS(s.config.WebhookTLSCert, s.config.WebhookTLSKey); err != nil && err != http.ErrServerClosed {
			klog.Errorf("Webhook server stopped unexpectedly: %v", err)
		}
	}()
	return nil
}

// Shutdown performs graceful shutdown of both the main and webhook HTTP servers.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.webhookServer != nil {
		klog.Info("Shutting down webhook server...")
		if err := s.webhookServer.Shutdown(ctx); err != nil {
			klog.Errorf("Webhook server shutdown error: %v", err)
		} else {
			klog.Info("Webhook server stopped")
		}
	}
	if s.httpServer != nil {
		klog.Info("Shutting down HTTP server...")
		if err := s.httpServer.Shutdown(ctx); err != nil {
			klog.Errorf("HTTP server shutdown error: %v", err)
			return fmt.Errorf("HTTP server shutdown: %w", err)
		}
		klog.Info("HTTP server stopped")
	} else {
		klog.Info("HTTP server not initialized, skipping HTTP shutdown")
	}
	return nil
}

// WaitForBackgroundWorkers blocks until all background workers (e.g. garbage collector)
// have finished their current operations and exited.
func (s *Server) WaitForBackgroundWorkers() {
	s.wg.Wait()
}

// CloseStore releases all resources held by the store (e.g. connection pools).
func (s *Server) CloseStore() error {
	klog.Info("Closing store connections...")
	if err := s.storeClient.Close(); err != nil {
		klog.Errorf("store close error: %v", err)
		return fmt.Errorf("store close: %w", err)
	}
	klog.Info("Store connections closed")
	return nil
}

// loggingMiddleware logs each request (except /health)
func (s *Server) loggingMiddleware(c *gin.Context) {
	start := time.Now()
	klog.Infof("%s %s %s", c.Request.Method, c.Request.RequestURI, c.ClientIP())
	c.Next()
	klog.Infof("%s %s - completed in %v", c.Request.Method, c.Request.RequestURI, time.Since(start))
}
