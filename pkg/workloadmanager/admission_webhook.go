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
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

// admissionHandler validates SnapStart objects on CREATE.
// Registered at /validate/snapstart on the Workload Manager's Gin server.
//
// Phase 1 validations:
//  1. runtimeRef.kind=AgentRuntime is rejected (Phase 2 only).
//  2. A second SnapStart referencing the same CodeInterpreter is rejected (uniqueness).
//
// To activate, apply a ValidatingWebhookConfiguration that points to this path
// with caBundle matching the TLS certificate used by the Workload Manager server.
type admissionHandler struct {
	indexer *snapStartIndexer
}

func newAdmissionHandler(indexer *snapStartIndexer) *admissionHandler {
	return &admissionHandler{indexer: indexer}
}

// handleValidateSnapStart is the Gin handler for POST /validate/snapstart.
func (ah *admissionHandler) handleValidateSnapStart(c *gin.Context) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 1<<20))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "read body: " + err.Error()})
		return
	}

	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unmarshal AdmissionReview: " + err.Error()})
		return
	}
	if review.Request == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "nil AdmissionRequest"})
		return
	}

	review.Response = ah.validate(review.Request)
	review.Response.UID = review.Request.UID

	c.JSON(http.StatusOK, review)
}

func (ah *admissionHandler) validate(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	switch req.Operation {
	case admissionv1.Create:
		return ah.validateCreate(req)
	case admissionv1.Update:
		return ah.validateUpdate(req)
	default:
		return admissionAllowed()
	}
}

func (ah *admissionHandler) validateCreate(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	var ss runtimev1alpha1.SnapStart
	if err := json.Unmarshal(req.Object.Raw, &ss); err != nil {
		klog.Warningf("admissionHandler: failed to unmarshal SnapStart: %v", err)
		return admissionDenied(fmt.Sprintf("invalid SnapStart object: %v", err))
	}

	// Phase 1: only runtimeRef.kind=CodeInterpreter is supported.
	if ss.Spec.RuntimeRef.Kind == "AgentRuntime" {
		return admissionDenied("runtimeRef.kind=AgentRuntime is not supported in Phase 1; SnapStart for AgentRuntime will be available in Phase 2")
	}

	// Phase 1: only NodeLocal artifact distribution is supported.
	if ss.Spec.Artifact != nil {
		switch ss.Spec.Artifact.Distribution {
		case "", runtimev1alpha1.DistributionModeNodeLocal:
			// ok
		default:
			return admissionDenied(fmt.Sprintf(
				"artifact.distribution=%s is not supported in Phase 1; only NodeLocal is available",
				ss.Spec.Artifact.Distribution))
		}
	}

	// Enforce uniqueness: at most one SnapStart per CodeInterpreter in a namespace.
	existing := ah.indexer.getByRuntime(req.Namespace, ss.Spec.RuntimeRef.Name)
	for _, e := range existing {
		if e.DeletionTimestamp == nil {
			return admissionDenied(fmt.Sprintf(
				"a SnapStart (%s) already references %s %q in namespace %q; delete it before creating a new one",
				e.Name, ss.Spec.RuntimeRef.Kind, ss.Spec.RuntimeRef.Name, req.Namespace))
		}
	}

	return admissionAllowed()
}

// immutable fields: runtimeRef.kind and runtimeRef.name may not change after creation.
func (ah *admissionHandler) validateUpdate(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	var newSS, oldSS runtimev1alpha1.SnapStart
	if err := json.Unmarshal(req.Object.Raw, &newSS); err != nil {
		return admissionDenied(fmt.Sprintf("invalid SnapStart object: %v", err))
	}
	if req.OldObject.Raw != nil {
		if err := json.Unmarshal(req.OldObject.Raw, &oldSS); err != nil {
			return admissionDenied(fmt.Sprintf("invalid old SnapStart object: %v", err))
		}
	}

	if newSS.Spec.RuntimeRef.Kind != oldSS.Spec.RuntimeRef.Kind {
		return admissionDenied(fmt.Sprintf(
			"runtimeRef.kind is immutable: cannot change from %q to %q",
			oldSS.Spec.RuntimeRef.Kind, newSS.Spec.RuntimeRef.Kind))
	}
	if newSS.Spec.RuntimeRef.Name != oldSS.Spec.RuntimeRef.Name {
		return admissionDenied(fmt.Sprintf(
			"runtimeRef.name is immutable: cannot change from %q to %q",
			oldSS.Spec.RuntimeRef.Name, newSS.Spec.RuntimeRef.Name))
	}

	// Phase 1: artifact.distribution cannot be changed to an unsupported mode.
	if newSS.Spec.Artifact != nil {
		switch newSS.Spec.Artifact.Distribution {
		case "", runtimev1alpha1.DistributionModeNodeLocal:
			// ok
		default:
			return admissionDenied(fmt.Sprintf(
				"artifact.distribution=%s is not supported in Phase 1; only NodeLocal is available",
				newSS.Spec.Artifact.Distribution))
		}
	}

	return admissionAllowed()
}

func admissionAllowed() *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: true,
		Result:  &metav1.Status{Code: http.StatusOK},
	}
}

func admissionDenied(reason string) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Code:    http.StatusForbidden,
			Message: reason,
			Reason:  metav1.StatusReasonForbidden,
		},
	}
}
