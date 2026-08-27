package node

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	nodev1 "github.com/rebeccapanel/rebecca-node/internal/proto/node/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const maxOperationReceipts = 2048

type operationReceipt struct {
	Method     string `json:"method"`
	Response   []byte `json:"response"`
	RecordedAt int64  `json:"recorded_at"`
}

type operationDeduper struct {
	path     string
	mu       sync.Mutex
	receipts map[string]operationReceipt
	inflight map[string]chan struct{}
}

func newOperationDeduper(path string) *operationDeduper {
	d := &operationDeduper{
		path:     path,
		receipts: make(map[string]operationReceipt),
		inflight: make(map[string]chan struct{}),
	}
	raw, err := os.ReadFile(path)
	if err == nil && json.Unmarshal(raw, &d.receipts) != nil {
		log.Printf("failed to load operation receipts")
	}
	return d
}

func (d *operationDeduper) unaryServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	operationID := requestOperationID(req)
	newResponse := mutationResponse(info.FullMethod)
	if operationID == "" || newResponse == nil {
		return handler(ctx, req)
	}
	key := info.FullMethod + "\x00" + operationID
	for {
		d.mu.Lock()
		if receipt, ok := d.receipts[key]; ok {
			d.mu.Unlock()
			response := newResponse()
			if err := proto.Unmarshal(receipt.Response, response); err == nil {
				return response, nil
			}
			d.mu.Lock()
			delete(d.receipts, key)
			d.mu.Unlock()
			continue
		}
		if done, ok := d.inflight[key]; ok {
			d.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				continue
			}
		}
		done := make(chan struct{})
		d.inflight[key] = done
		d.mu.Unlock()

		response, err := handler(ctx, req)
		d.mu.Lock()
		if err == nil {
			if message, ok := response.(proto.Message); ok {
				if raw, marshalErr := proto.Marshal(message); marshalErr == nil {
					d.receipts[key] = operationReceipt{Method: info.FullMethod, Response: raw, RecordedAt: time.Now().Unix()}
					d.trimLocked()
					d.saveLocked()
				}
			}
		}
		delete(d.inflight, key)
		close(done)
		d.mu.Unlock()
		return response, err
	}
}

func requestOperationID(request any) string {
	type operationRequest interface{ GetOperationId() string }
	if value, ok := request.(operationRequest); ok {
		return strings.TrimSpace(value.GetOperationId())
	}
	return ""
}

func mutationResponse(method string) func() proto.Message {
	switch {
	case strings.HasSuffix(method, "/ConfigureWindscribe"):
		return func() proto.Message { return &nodev1.WindscribeProxyResponse{} }
	case strings.HasSuffix(method, "/ConfigurePsiphon"):
		return func() proto.Message { return &nodev1.PsiphonProxyResponse{} }
	case strings.HasSuffix(method, "/StartRuntime"),
		strings.HasSuffix(method, "/RestartRuntime"),
		strings.HasSuffix(method, "/StopRuntime"),
		strings.HasSuffix(method, "/SyncConfig"),
		strings.HasSuffix(method, "/AddUser"),
		strings.HasSuffix(method, "/UpdateUser"),
		strings.HasSuffix(method, "/RemoveUser"),
		strings.HasSuffix(method, "/UpdateRuntime"),
		strings.HasSuffix(method, "/UpdateGeo"),
		strings.HasSuffix(method, "/RestartService"),
		strings.HasSuffix(method, "/UpdateService"),
		strings.HasSuffix(method, "/RebootHost"),
		strings.HasSuffix(method, "/ApplyIPBlocks"),
		strings.HasSuffix(method, "/ApplyTorProxy"):
		return func() proto.Message { return &nodev1.RuntimeActionResponse{} }
	default:
		return nil
	}
}

func (d *operationDeduper) trimLocked() {
	if len(d.receipts) <= maxOperationReceipts {
		return
	}
	keys := make([]string, 0, len(d.receipts))
	for key := range d.receipts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return d.receipts[keys[i]].RecordedAt < d.receipts[keys[j]].RecordedAt })
	for _, key := range keys[:len(keys)-maxOperationReceipts] {
		delete(d.receipts, key)
	}
}

func (d *operationDeduper) saveLocked() {
	raw, err := json.Marshal(d.receipts)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(d.path), 0o700); err != nil {
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(d.path), ".operation-receipts-*")
	if err != nil {
		return
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(raw)
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmpPath, d.path)
	}
	if err != nil {
		log.Printf("failed to save operation receipts: %v", err)
	}
}
