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

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	redisv9 "github.com/redis/go-redis/v9"

	"github.com/volcano-sh/agentcube/pkg/common/types"
)

type redisStore struct {
	cli                  *redisv9.Client
	sessionPrefix        string
	expiryIndexKey       string
	lastActivityIndexKey string
}

// initRedisStore init redis store client
func initRedisStore() (*redisStore, error) {
	redisOptions, err := makeRedisOptions()
	if err != nil {
		return nil, fmt.Errorf("make redis options failed: %w", err)
	}

	return &redisStore{
		cli:                  redisv9.NewClient(redisOptions),
		sessionPrefix:        "session:",
		expiryIndexKey:       "session:expiry",
		lastActivityIndexKey: "session:last_activity",
	}, nil
}

// makeRedisOptions creates redis options from environment variables
func makeRedisOptions() (*redisv9.Options, error) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		return nil, fmt.Errorf("missing env var REDIS_ADDR")
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")
	// Secure-by-default: require non-empty password unless explicitly disabled via REDIS_PASSWORD_REQUIRED=false.
	if strings.ToLower(os.Getenv("REDIS_PASSWORD_REQUIRED")) != "false" && redisPassword == "" {
		return nil, fmt.Errorf("REDIS_PASSWORD is required but not set")
	}

	return &redisv9.Options{
		Addr:     redisAddr,
		Password: redisPassword,
	}, nil
}

// sessionKey make sessionKey by sessionID
func (rs *redisStore) sessionKey(sessionID string) string {
	return rs.sessionPrefix + sessionID
}

// loadSandboxesBySessionIDs loads sandbox objects for the given session IDs.
func (rs *redisStore) loadSandboxesBySessionIDs(ctx context.Context, sessionIDs []string) ([]*types.SandboxInfo, error) {
	if len(sessionIDs) == 0 {
		return nil, nil
	}

	sandboxCommands := make([]*redisv9.StringCmd, len(sessionIDs))
	pipe := rs.cli.Pipeline()
	for i, sessionID := range sessionIDs {
		sessionKey := rs.sessionKey(sessionID)
		sandboxCommands[i] = pipe.Get(ctx, sessionKey)
	}
	_, pipeErr := pipe.Exec(ctx)
	if pipeErr != nil && !errors.Is(pipeErr, redisv9.Nil) {
		return nil, fmt.Errorf("redis pipeline exec failed: %w", pipeErr)
	}

	result := make([]*types.SandboxInfo, 0, len(sessionIDs))
	for i, cmd := range sandboxCommands {
		data, err := cmd.Bytes()
		if errors.Is(err, redisv9.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("loadSandboxesBySessionIDs: get sandbox JSON for session %s: %w", sessionIDs[i], err)
		}
		var sandboxRedis types.SandboxInfo
		if err := json.Unmarshal(data, &sandboxRedis); err != nil {
			return nil, fmt.Errorf("loadSandboxesBySessionIDs: unmarshal sandbox for session %s: %w", sessionIDs[i], err)
		}
		result = append(result, &sandboxRedis)
	}

	return result, nil
}

func (rs *redisStore) Ping(ctx context.Context) error {
	resp, err := rs.cli.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("ping error: %w", err)
	}
	if resp != "PONG" {
		return fmt.Errorf("unexpected ping response: %s", resp)
	}
	return nil
}

// GetSandboxBySessionID looks up the sandbox bound to the given session ID.
// Underlying Redis: GET session:{sessionID} -> Sandbox Info(JSON).
func (rs *redisStore) GetSandboxBySessionID(ctx context.Context, sessionID string) (*types.SandboxInfo, error) {
	key := rs.sessionKey(sessionID)

	b, err := rs.cli.Get(ctx, key).Bytes()
	if errors.Is(err, redisv9.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("GetSandboxBySessionID: redis GET %s failed: %w", key, err)
	}

	var sandboxRedis types.SandboxInfo
	if err := json.Unmarshal(b, &sandboxRedis); err != nil {
		return nil, fmt.Errorf("GetSandboxBySessionID: unmarshal sandbox failed: %w", err)
	}
	return &sandboxRedis, nil
}

func (rs *redisStore) StoreSandbox(ctx context.Context, sandboxRedis *types.SandboxInfo) error {
	if sandboxRedis == nil {
		return errors.New("StoreSandbox: sandbox is nil")
	}

	sessionKey := rs.sessionKey(sandboxRedis.SessionID)

	b, err := json.Marshal(sandboxRedis)
	if err != nil {
		return fmt.Errorf("StoreSandbox: marshal sandbox failed: %w", err)
	}

	if sandboxRedis.ExpiresAt.IsZero() {
		return fmt.Errorf("StoreSandbox: sandbox expired at is zero")
	}

	pipe := rs.cli.Pipeline()
	pipe.SetNX(ctx, sessionKey, b, 0)
	pipe.ZAdd(ctx, rs.expiryIndexKey, redisv9.Z{
		Score:  float64(sandboxRedis.ExpiresAt.Unix()),
		Member: sandboxRedis.SessionID,
	})
	pipe.ZAdd(ctx, rs.lastActivityIndexKey, redisv9.Z{
		Score:  float64(time.Now().Unix()),
		Member: sandboxRedis.SessionID,
	})

	cmder, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("StoreSandbox: redis Pipeline EXEC: %w", err)
	}

	if len(cmder) == 0 {
		return errors.New("StoreSandbox: unexpected empty cmder")
	}

	for i, cmd := range cmder {
		if err = cmd.Err(); err != nil {
			return fmt.Errorf("StoreSandbox: EXEC pipeline failed: %w, cmder index: %v", err, i)
		}
	}

	return nil
}

// UpdateSandbox update sandbox obj in redis
// update sandbox object only, do not update expiry and lastActivity ZSet
func (rs *redisStore) UpdateSandbox(ctx context.Context, sandboxRedis *types.SandboxInfo) error {
	if sandboxRedis == nil {
		return errors.New("UpdateSandbox: sandbox is nil")
	}

	sessionKey := rs.sessionKey(sandboxRedis.SessionID)

	b, err := json.Marshal(sandboxRedis)
	if err != nil {
		return fmt.Errorf("UpdateSandbox: marshal sandbox: %w", err)
	}

	ok, err := rs.cli.SetXX(ctx, sessionKey, b, 0).Result()
	if err != nil {
		return fmt.Errorf("UpdateSandbox: redis SETXX %s: %w", sessionKey, err)
	}

	if !ok {
		return fmt.Errorf("UpdateSandbox: redis SETXX %s, key does not exist", sessionKey)
	}
	return nil
}

func (rs *redisStore) DeleteSandboxBySessionID(ctx context.Context, sessionID string) error {
	sessionKey := rs.sessionKey(sessionID)

	pipe := rs.cli.Pipeline()
	pipe.Del(ctx, sessionKey)
	pipe.ZRem(ctx, rs.expiryIndexKey, sessionID)
	pipe.ZRem(ctx, rs.lastActivityIndexKey, sessionID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("DeleteSandboxBySessionID: pipeline EXEC: %w", err)
	}
	return nil
}

// ListExpiredSandboxes returns up to limit sandboxes whose ExpiresAt is before.
// It uses a sorted-set index and is linear in the number of results.
func (rs *redisStore) ListExpiredSandboxes(ctx context.Context, before time.Time, limit int64) ([]*types.SandboxInfo, error) {
	if limit <= 0 {
		return nil, nil
	}

	maxScore := before.Unix()
	ids, err := rs.cli.ZRangeByScore(ctx, rs.expiryIndexKey, &redisv9.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", maxScore),
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("ListExpiredSandboxes: ZRangeByScore failed: %w", err)
	}

	return rs.loadSandboxesBySessionIDs(ctx, ids)
}

// ListInactiveSandboxes returns up to limit sandboxes whose last activity
// time is before, using the last-activity sorted-set index.
// LastActivityAt is populated on each returned SandboxInfo from the sorted-set
// score so the caller can apply per-sandbox idle-timeout logic.
func (rs *redisStore) ListInactiveSandboxes(ctx context.Context, before time.Time, limit int64) ([]*types.SandboxInfo, error) {
	if limit <= 0 {
		return nil, nil
	}

	maxScore := before.Unix()
	zs, err := rs.cli.ZRangeByScoreWithScores(ctx, rs.lastActivityIndexKey, &redisv9.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", maxScore),
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("ListInactiveSandboxes: ZRangeByScoreWithScores failed: %w", err)
	}

	ids := make([]string, len(zs))
	scores := make(map[string]time.Time, len(zs))
	for i, z := range zs {
		id, ok := z.Member.(string)
		if !ok {
			return nil, fmt.Errorf("ListInactiveSandboxes: unexpected member type %T", z.Member)
		}
		ids[i] = id
		scores[id] = time.Unix(int64(z.Score), 0)
	}

	sandboxes, err := rs.loadSandboxesBySessionIDs(ctx, ids)
	if err != nil {
		return nil, err
	}

	for _, s := range sandboxes {
		if t, ok := scores[s.SessionID]; ok {
			s.LastActivityAt = t
		}
	}
	return sandboxes, nil
}

// Close releases all resources held by the redis store.
func (rs *redisStore) Close() error {
	return rs.cli.Close()
}

// snapshotKey returns the Redis key for the snapshot Hash of a given runtime.
func snapshotKey(namespace, name string) string {
	return fmt.Sprintf("snapshot:%s:%s", namespace, name)
}

// StoreSnapshot writes or updates the snapshot record for a specific (runtime, node) pair.
func (rs *redisStore) StoreSnapshot(ctx context.Context, namespace, name string, info *types.SnapshotInfo) error {
	if info == nil {
		return errors.New("StoreSnapshot: info is nil")
	}
	key := snapshotKey(namespace, name)
	b, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("StoreSnapshot: marshal info: %w", err)
	}
	if err := rs.cli.HSet(ctx, key, info.NodeName, string(b)).Err(); err != nil {
		return fmt.Errorf("StoreSnapshot: HSET %s %s: %w", key, info.NodeName, err)
	}
	return nil
}

// GetSnapshotNodes returns all per-node snapshot records for the given runtime.
func (rs *redisStore) GetSnapshotNodes(ctx context.Context, namespace, name string) ([]*types.SnapshotInfo, error) {
	key := snapshotKey(namespace, name)
	result, err := rs.cli.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("GetSnapshotNodes: HGETALL %s: %w", key, err)
	}
	infos := make([]*types.SnapshotInfo, 0, len(result))
	for _, v := range result {
		var info types.SnapshotInfo
		if err := json.Unmarshal([]byte(v), &info); err != nil {
			return nil, fmt.Errorf("GetSnapshotNodes: unmarshal: %w", err)
		}
		infos = append(infos, &info)
	}
	return infos, nil
}

// DeleteSnapshot removes the snapshot record for a specific (runtime, node) pair.
func (rs *redisStore) DeleteSnapshot(ctx context.Context, namespace, name, nodeName string) error {
	key := snapshotKey(namespace, name)
	if err := rs.cli.HDel(ctx, key, nodeName).Err(); err != nil {
		return fmt.Errorf("DeleteSnapshot: HDEL %s %s: %w", key, nodeName, err)
	}
	return nil
}

// DeleteAllSnapshots removes all per-node snapshot records for a runtime.
func (rs *redisStore) DeleteAllSnapshots(ctx context.Context, namespace, name string) error {
	key := snapshotKey(namespace, name)
	if err := rs.cli.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("DeleteAllSnapshots: DEL %s: %w", key, err)
	}
	return nil
}

// ListSnapshotTemplateIDs returns all tracked template IDs across all runtimes and nodes.
func (rs *redisStore) ListSnapshotTemplateIDs(ctx context.Context) ([]string, error) {
	var cursor uint64
	var ids []string
	for {
		keys, nextCursor, err := rs.cli.Scan(ctx, cursor, "snapshot:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("ListSnapshotTemplateIDs: SCAN: %w", err)
		}
		for _, key := range keys {
			fields, err := rs.cli.HGetAll(ctx, key).Result()
			if err != nil {
				continue
			}
			for _, v := range fields {
				var info types.SnapshotInfo
				if err := json.Unmarshal([]byte(v), &info); err != nil {
					continue
				}
				if info.TemplateID != "" {
					ids = append(ids, info.TemplateID)
				}
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return ids, nil
}

// ListAllSnapshotKeys returns all [namespace, name] pairs that have snapshot data.
// It scans keys matching "snapshot:*" and parses the namespace and name from each key.
func (rs *redisStore) ListAllSnapshotKeys(ctx context.Context) ([][2]string, error) {
	var cursor uint64
	var pairs [][2]string
	for {
		keys, nextCursor, err := rs.cli.Scan(ctx, cursor, "snapshot:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("ListAllSnapshotKeys: SCAN: %w", err)
		}
		for _, key := range keys {
			rest := strings.TrimPrefix(key, "snapshot:")
			idx := strings.Index(rest, ":")
			if idx < 0 {
				continue
			}
			pairs = append(pairs, [2]string{rest[:idx], rest[idx+1:]})
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return pairs, nil
}

// UpdateSessionLastActivity updates the last-activity index for the given session.
func (rs *redisStore) UpdateSessionLastActivity(ctx context.Context, sessionID string, at time.Time) error {
	if sessionID == "" {
		return errors.New("UpdateSessionLastActivity: sessionID is empty")
	}
	if at.IsZero() {
		at = time.Now()
	}

	// Ensure the sandbox mapping exists; otherwise treat as not found.
	sessionKey := rs.sessionKey(sessionID)
	_, err := rs.cli.Get(ctx, sessionKey).Result()
	if errors.Is(err, redisv9.Nil) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("UpdateSessionLastActivity: get mapping for sessionID %s: %w", sessionID, err)
	}

	if _, err := rs.cli.ZAdd(ctx, rs.lastActivityIndexKey, redisv9.Z{
		Score:  float64(at.Unix()),
		Member: sessionID,
	}).Result(); err != nil {
		return fmt.Errorf("UpdateSessionLastActivity: ZAdd: %w", err)
	}

	return nil
}
