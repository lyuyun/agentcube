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

package main

import (
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	agentdriver "github.com/volcano-sh/agentcube/pkg/agentd/driver"
	agentcontroller "github.com/volcano-sh/agentcube/pkg/agentd/controller"
	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

var (
	schemeBuilder = runtime.NewScheme()
)

func init() {
	utilruntime.Must(scheme.AddToScheme(schemeBuilder))
	utilruntime.Must(sandboxv1alpha1.AddToScheme(schemeBuilder))
	utilruntime.Must(runtimev1alpha1.AddToScheme(schemeBuilder))
}

func main() {
	klog.InitFlags(nil)
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: schemeBuilder,
		Metrics: metricsserver.Options{
			BindAddress: "0", // Disable metrics server
		},
		HealthProbeBindAddress: "0", // Disable health probe server

	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to start manager: %v\n", err)
		os.Exit(1)
	}

	// Read the node name from the downward API environment variable.
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		fmt.Fprintf(os.Stderr, "NODE_NAME environment variable is required\n")
		os.Exit(1)
	}

	driver := agentdriver.NewGRPCDriver(os.Getenv("DRIVER_SOCKET"))

	// Advertise snapshot capabilities on the node before controllers start so that
	// the workload manager can select this node for snapshot builds immediately.
	cs, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create kubernetes client: %v\n", err)
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	if err := agentdriver.AdvertiseDriverCapabilities(ctx, cs, nodeName, driver); err != nil {
		fmt.Fprintf(os.Stderr, "unable to advertise driver capabilities: %v\n", err)
		os.Exit(1)
	}

	if err := (&agentcontroller.SnapshotTaskController{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
		NodeName:  nodeName,
		Driver:    driver,
	}).SetupWithManager(mgr); err != nil {
		fmt.Fprintf(os.Stderr, "unable to create snapshot task controller: %v\n", err)
		os.Exit(1)
	}

	if err := mgr.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "problem running manager: %v\n", err)
		os.Exit(1)
	}
}
