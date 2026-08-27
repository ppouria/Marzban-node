package node

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	appconfig "github.com/rebeccapanel/rebecca-node/internal/config"
	nodev1 "github.com/rebeccapanel/rebecca-node/internal/proto/node/v1"
	"google.golang.org/grpc"
)

func TestOperationDeduperKeepsBoundedRecentReceiptWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipts.jsonl")
	deduper := newOperationDeduper(path)
	info := &grpc.UnaryServerInfo{FullMethod: "/rebecca.node.v1.NodeRuntimeService/RestartService"}
	handler := func(context.Context, any) (any, error) {
		return &nodev1.RuntimeActionResponse{Accepted: true}, nil
	}
	for i := 0; i < maxOperationReceipts+52; i++ {
		req := &nodev1.ServiceRestartRequest{OperationId: fmt.Sprintf("operation-%d", i)}
		if _, err := deduper.unaryServerInterceptor(context.Background(), req, info, handler); err != nil {
			t.Fatal(err)
		}
	}

	reloaded := newOperationDeduper(path)
	if len(reloaded.receipts) != maxOperationReceipts {
		t.Fatalf("loaded receipts = %d, want %d", len(reloaded.receipts), maxOperationReceipts)
	}
	if _, ok := reloaded.receipts[info.FullMethod+"\x00operation-"+fmt.Sprint(maxOperationReceipts+51)]; !ok {
		t.Fatal("newest operation receipt was not retained")
	}
}

func TestOperationDeduperPersistsSuccessfulMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipts.json")
	info := &grpc.UnaryServerInfo{FullMethod: "/rebecca.node.v1.NodeRuntimeService/RestartService"}
	req := &nodev1.ServiceRestartRequest{OperationId: "operation-1"}
	var calls atomic.Int32
	handler := func(context.Context, any) (any, error) {
		calls.Add(1)
		return &nodev1.RuntimeActionResponse{OperationId: "operation-1", Accepted: true}, nil
	}

	if _, err := newOperationDeduper(path).unaryServerInterceptor(context.Background(), req, info, handler); err != nil {
		t.Fatal(err)
	}
	response, err := newOperationDeduper(path).unaryServerInterceptor(context.Background(), req, info, handler)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || !response.(*nodev1.RuntimeActionResponse).GetAccepted() {
		t.Fatalf("duplicate operation executed again: calls=%d response=%v", calls.Load(), response)
	}
}

func TestOperationDeduperCoalescesConcurrentMutation(t *testing.T) {
	deduper := newOperationDeduper(filepath.Join(t.TempDir(), "receipts.json"))
	info := &grpc.UnaryServerInfo{FullMethod: "/rebecca.node.v1.NodeRuntimeService/RebootHost"}
	req := &nodev1.HostRebootRequest{OperationId: "operation-2"}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	handler := func(context.Context, any) (any, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return &nodev1.RuntimeActionResponse{OperationId: "operation-2", Accepted: true}, nil
	}
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := deduper.unaryServerInterceptor(context.Background(), req, info, handler); err != nil {
				t.Errorf("mutation failed: %v", err)
			}
		}()
	}
	<-started
	close(release)
	wait.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent duplicate executed %d times", calls.Load())
	}
}

func TestRevisionInterceptorRejectsStaleConfig(t *testing.T) {
	server := &Server{settings: appconfig.Settings{RebeccaDataDir: t.TempDir()}}
	server.saveConfigCache(`{"inbounds":[]}`, "127.0.0.1", nil, nil, nil, nil)
	server.setAppliedRevision(7)
	err := server.validateDesiredRevision(&nodev1.RuntimeConfigRequest{DesiredRevision: 6})
	if err == nil {
		t.Fatal("stale revision was accepted")
	}
}
