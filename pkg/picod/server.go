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

package picod

import (
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

// snapstartBuildMode is true when AGENTCUBE_SNAPSTART_BUILD=true, indicating
// this picod instance is a snapshot build sandbox and should implement
// the WarmFork Ready-Waiting protocol.
var snapstartBuildMode = os.Getenv("AGENTCUBE_SNAPSTART_BUILD") == "true"

// safeToSnapshot is atomically set to 1 once picod has entered the
// InjectionWaiting state (accept() loop is running, no user state loaded).
var safeToSnapshot int32

const (
	// MaxBodySize limits request body size to prevent memory exhaustion
	MaxBodySize = 32 << 20 // 32 MB
)

// Config defines server configuration
type Config struct {
	Port      int    `json:"port"`
	Workspace string `json:"workspace"`
}

// Server defines the PicoD HTTP server
type Server struct {
	engine       *gin.Engine
	config       Config
	authManager  *AuthManager
	startTime    time.Time
	workspaceDir string
	// snapReady is closed once picod has entered InjectionWaiting state.
	// Used to signal the HTTP goroutine that safeToSnapshot can be set.
	snapReady chan struct{}
	// activeTaskCount tracks the number of in-flight /api/execute requests.
	// Accessed atomically; must not be baked into a WarmFork snapshot.
	activeTaskCount int32
}

// NewServer creates a new PicoD server instance
func NewServer(config Config) *Server {
	s := &Server{
		config:      config,
		startTime:   time.Now(),
		authManager: NewAuthManager(),
		snapReady:   make(chan struct{}),
	}

	// Initialize workspace directory
	klog.Infof("Initializing workspace with config.Workspace: %q", config.Workspace)
	workspaceDir := config.Workspace
	if workspaceDir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			klog.Fatalf("Failed to get current working directory: %v", err)
		}
		workspaceDir = cwd
	}
	if err := s.setWorkspace(workspaceDir); err != nil {
		klog.Fatalf("Failed to initialize workspace: %v", err)
	}
	klog.Infof("Final workspace directory: %q", s.workspaceDir)

	// Disable Gin debug output in production mode
	gin.SetMode(gin.ReleaseMode)

	engine := gin.New()

	// Global middleware
	engine.Use(gin.Logger())   // Request logging
	engine.Use(gin.Recovery()) // Crash recovery
	// Limit request body size to prevent DoS attacks.
	// First reject requests whose Content-Length already exceeds the limit,
	// then wrap the body with MaxBytesReader as a safety net for chunked
	// transfers or requests without Content-Length.
	engine.Use(func(c *gin.Context) {
		if c.Request.ContentLength > MaxBodySize {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{
				"error":  "request body too large",
				"detail": fmt.Sprintf("maximum allowed size is %d bytes", MaxBodySize),
			})
			c.Abort()
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, MaxBodySize)
		c.Next()
	})
	engine.MaxMultipartMemory = MaxBodySize

	// Load public key from environment variable (required)
	if err := s.authManager.LoadPublicKeyFromEnv(); err != nil {
		klog.Fatalf("Failed to load public key from environment: %v", err)
	}

	// Snapshot build sandboxes must never serve user traffic. Otherwise a write can
	// race with template creation after /runtime/status reported a clean checkpoint.
	if !snapstartBuildMode {
		api := engine.Group("/api")
		api.Use(s.authManager.AuthMiddleware())
		{
			api.POST("/execute", s.ExecuteHandler)
			api.POST("/files", s.UploadFileHandler)
			api.GET("/files", s.ListFilesHandler)
			api.GET("/files/*path", s.DownloadFileHandler)
		}
	}

	// Health check (no authentication required)
	engine.GET("/health", s.HealthCheckHandler)

	// /runtime/status is only registered when running in snapstart build mode.
	// In cold-start or restore mode this endpoint does not exist (404), which is
	// the signal to SnapshotController that the runtime doesn't implement the protocol.
	if snapstartBuildMode {
		engine.GET("/runtime/status", s.RuntimeStatusHandler)
	}

	s.engine = engine
	return s
}

// Run starts the server. In snapstart build mode it also starts the
// WarmFork inject-socket listener in the background.
func (s *Server) Run() error {
	addr := fmt.Sprintf(":%d", s.config.Port)
	klog.Infof("PicoD server starting on %s", addr)

	if snapstartBuildMode {
		go s.runInjectSocket()
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           s.engine,
		ReadHeaderTimeout: 10 * time.Second, // Prevent Slowloris attacks
	}

	return server.ListenAndServe()
}

// RuntimeStatusHandler handles GET /runtime/status.
// Returns safeToSnapshot=true only after picod has entered the InjectionWaiting state,
// the workspace is empty (no user files), and no execute tasks are in flight.
func (s *Server) RuntimeStatusHandler(c *gin.Context) {
	injectReady := atomic.LoadInt32(&safeToSnapshot) == 1
	activeTasks := atomic.LoadInt32(&s.activeTaskCount)
	workspaceEmpty := s.isWorkspaceEmpty()
	c.JSON(http.StatusOK, gin.H{
		"checkpoint":      "InterpreterReady",
		"safeToSnapshot":  injectReady && activeTasks == 0 && workspaceEmpty,
		"userStateLoaded": !workspaceEmpty || activeTasks > 0,
		"activeTasks":     int(activeTasks),
		"details": gin.H{
			"interpreterState": "idle",
			"workspaceEmpty":   workspaceEmpty,
		},
	})
}

// isWorkspaceEmpty reports whether the workspace directory contains no files.
// A non-empty workspace means user state has been written and the instance must
// not be snapshotted.
func (s *Server) isWorkspaceEmpty() bool {
	entries, err := os.ReadDir(s.workspaceDir)
	if err != nil {
		klog.V(4).Infof("picod: isWorkspaceEmpty: ReadDir %s: %v", s.workspaceDir, err)
		return false
	}
	return len(entries) == 0
}

// HealthCheckHandler handles health check requests
func (s *Server) HealthCheckHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "PicoD",
		"version": "0.0.1",
		"uptime":  time.Since(s.startTime).String(),
	})
}
