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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// defaultKuasarAdminPort is the port agentd exposes the Kuasar Admin proxy on.
	defaultKuasarAdminPort = 9090
	// kuasarAdminPath is the HTTP path for the Kuasar Admin proxy.
	kuasarAdminPath = "/kuasar-admin"
)

// KuasarAdminClient is an HTTP client that calls the agentd Kuasar Admin Proxy
// on a given node to perform create-template / delete-template / list-templates.
type KuasarAdminClient struct {
	nodeIP string
	port   int
	token  string
	client *http.Client
}

// NewKuasarAdminClient creates a new KuasarAdminClient for the given node.
// token is the shared Bearer token sent with every request.
func NewKuasarAdminClient(nodeIP, token string) *KuasarAdminClient {
	return &KuasarAdminClient{
		nodeIP: nodeIP,
		port:   defaultKuasarAdminPort,
		token:  token,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// TemplateCreateRequest is the JSON body for a template-create action.
type TemplateCreateRequest struct {
	Action       string            `json:"action"` // always "template-create"
	SandboxID    string            `json:"sandbox_id"`
	SnapshotType string            `json:"snapshot_type"` // "warm_fork"
	Key          string            `json:"key"`
	Owner        map[string]string `json:"owner"`
}

// templateCreateResponse is the JSON response from a successful template-create.
type templateCreateResponse struct {
	OK         bool   `json:"ok"`
	TemplateID string `json:"template_id"`
	Error      string `json:"error,omitempty"`
}

// templateDeleteRequest is the JSON body for a delete-template action.
type templateDeleteRequest struct {
	Action     string `json:"action"` // always "delete-template"
	TemplateID string `json:"template_id"`
}

// templateDeleteResponse is the JSON response from a delete-template action.
type templateDeleteResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// templateListRequest is the JSON body for a list-templates action.
type templateListRequest struct {
	Action string `json:"action"` // always "list-templates"
}

// templateListResponse is the JSON response from a list-templates action.
type templateListResponse struct {
	OK        bool                  `json:"ok"`
	Templates []*KuasarTemplateInfo `json:"templates"`
	Error     string                `json:"error,omitempty"`
}

// KuasarTemplateInfo describes a single snapshot template returned by list-templates.
type KuasarTemplateInfo struct {
	ID           string            `json:"template_id"`
	Key          string            `json:"key"`
	SnapshotType string            `json:"snapshot_type"`
	CreatedAt    time.Time         `json:"created_at"`
	LeaseCount   int               `json:"lease_count"`
	Owner        map[string]string `json:"owner,omitempty"`
}

// baseURL returns the base URL for the agentd proxy on this node.
func (c *KuasarAdminClient) baseURL() string {
	return fmt.Sprintf("http://%s:%d%s", c.nodeIP, c.port, kuasarAdminPath)
}

// doRequest performs an HTTP POST to the agentd proxy with JSON body.
func (c *KuasarAdminClient) doRequest(ctx context.Context, body interface{}) ([]byte, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL(), bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP POST %s: %w", c.baseURL(), err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(data))
	}

	return data, nil
}

// CreateTemplate calls the Kuasar Admin API to create a WarmFork snapshot template
// for the given sandboxID. Returns the assigned templateID.
func (c *KuasarAdminClient) CreateTemplate(ctx context.Context, sandboxID, templateKey string, owner map[string]string) (string, error) {
	reqBody := TemplateCreateRequest{
		Action:       "create-template",
		SandboxID:    sandboxID,
		SnapshotType: "warm_fork",
		Key:          templateKey,
		Owner:        owner,
	}

	data, err := c.doRequest(ctx, reqBody)
	if err != nil {
		return "", fmt.Errorf("CreateTemplate: %w", err)
	}

	var resp templateCreateResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", fmt.Errorf("CreateTemplate: unmarshal response: %w", err)
	}
	if !resp.OK {
		return "", fmt.Errorf("CreateTemplate: %s", resp.Error)
	}
	return resp.TemplateID, nil
}

// DeleteTemplate calls the Kuasar Admin API to delete the snapshot template with the given ID.
// Returns an error that satisfies isTemplateInUse() if the template still has active leases.
func (c *KuasarAdminClient) DeleteTemplate(ctx context.Context, templateID string) error {
	reqBody := templateDeleteRequest{
		Action:     "delete-template",
		TemplateID: templateID,
	}

	data, err := c.doRequest(ctx, reqBody)
	if err != nil {
		return fmt.Errorf("DeleteTemplate: %w", err)
	}

	var resp templateDeleteResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("DeleteTemplate: unmarshal response: %w", err)
	}
	if !resp.OK {
		return fmt.Errorf("DeleteTemplate: %s", resp.Error)
	}
	return nil
}

// ListTemplates calls the Kuasar Admin API to list all snapshot templates on this node.
func (c *KuasarAdminClient) ListTemplates(ctx context.Context) ([]*KuasarTemplateInfo, error) {
	reqBody := templateListRequest{Action: "list-templates"}

	data, err := c.doRequest(ctx, reqBody)
	if err != nil {
		return nil, fmt.Errorf("ListTemplates: %w", err)
	}

	var resp templateListResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("ListTemplates: unmarshal response: %w", err)
	}
	if !resp.OK {
		return nil, fmt.Errorf("ListTemplates: %s", resp.Error)
	}
	return resp.Templates, nil
}

// isTemplateInUse returns true if the error indicates the template has active leases.
func isTemplateInUse(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "template_in_use") || strings.Contains(err.Error(), "in use")
}

// isNotFound returns true if the error indicates the template was not found.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "not_found") || strings.Contains(err.Error(), "not found")
}
