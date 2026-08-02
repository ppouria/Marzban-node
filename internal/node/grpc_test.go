package node

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	appconfig "github.com/rebeccapanel/rebecca-node/internal/config"
	nodev1 "github.com/rebeccapanel/rebecca-node/internal/proto/node/v1"
	"github.com/rebeccapanel/rebecca-node/internal/xray"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestGRPCVPNRuntimeClearsMissingAuxiliaryState(t *testing.T) {
	ov, l2tp, pptp, wg, ikev2, anyConnect, err := grpcVPNRuntime(&nodev1.RuntimeConfigRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if ov == nil || l2tp == nil || pptp == nil || wg == nil || ikev2 == nil || anyConnect == nil {
		t.Fatal("missing runtime payload must clear every auxiliary runtime")
	}
	if len(ov.Inbounds)+len(l2tp.Inbounds)+len(pptp.Inbounds)+len(wg.Inbounds)+len(ikev2.Inbounds)+len(anyConnect.Inbounds) != 0 {
		t.Fatal("missing runtime payload must not restore cached auxiliary inbounds")
	}
}

func TestGRPCServerAcceptsMutualTLSClient(t *testing.T) {
	tempDir := t.TempDir()
	serverCertFile, serverKeyFile := writeSelfSignedCert(t, tempDir, "server", []string{"rebecca-node.test"})
	clientCertFile, clientKeyFile := writeSelfSignedCert(t, tempDir, "client", nil)

	settings := appconfig.Settings{
		AppName:           "rebecca-node",
		InstallMode:       "binary",
		NodeVersion:       "0.2.2",
		SSLCertFile:       serverCertFile,
		SSLKeyFile:        serverKeyFile,
		SSLClientCertFile: clientCertFile,
	}
	tlsConfig, err := loadGRPCServerTLS(settings)
	if err != nil {
		t.Fatalf("failed to load gRPC TLS config: %v", err)
	}

	server := &Server{
		settings: settings,
		core:     &xray.Core{},
		usage:    newUsageBuffer(),
		system:   newSystemSampler(),
		sessions: make(map[string]time.Time),
	}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	server.registerGRPC(grpcServer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("gRPC test server stopped: %v", err)
		}
	}()
	defer grpcServer.Stop()

	clientCert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		t.Fatalf("failed to load client cert: %v", err)
	}
	serverRootPEM, err := os.ReadFile(serverCertFile)
	if err != nil {
		t.Fatalf("failed to read server cert: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(serverRootPEM) {
		t.Fatal("failed to add server cert root")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		listener.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			ServerName:   "rebecca-node.test",
			RootCAs:      roots,
			Certificates: []tls.Certificate{clientCert},
			MinVersion:   tls.VersionTLS12,
		})),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("failed to dial gRPC server: %v", err)
	}
	defer conn.Close()

	control := nodev1.NewNodeControlServiceClient(conn)
	hello, err := control.Hello(ctx, &nodev1.HelloRequest{MasterId: "test-master"})
	if err != nil {
		t.Fatalf("hello failed: %v", err)
	}
	if hello.GetNodeVersion() != "0.2.2" || hello.GetInstallMode() != "binary" {
		t.Fatalf("unexpected hello response: %#v", hello)
	}
	t.Logf("hello: node_version=%s install_mode=%s started=%v", hello.GetNodeVersion(), hello.GetInstallMode(), hello.GetRuntime().GetStarted())

	connected, err := control.Connect(ctx, &nodev1.ConnectRequest{MasterId: "test-master"})
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	if connected.GetConnectionId() == "" || !connected.GetRuntime().GetConnected() {
		t.Fatalf("unexpected connect response: %#v", connected)
	}

	health, err := control.Health(ctx, &nodev1.HealthRequest{IncludeMetrics: true})
	if err != nil {
		t.Fatalf("health failed: %v", err)
	}
	if health.GetMetrics().GetSystem().GetCpuCores() == 0 {
		t.Fatalf("expected health metrics, got %#v", health)
	}
	metrics := health.GetMetrics().GetSystem()
	t.Logf(
		"health: connected=%v started=%v cpu_cores=%d cpu_usage=%.1f memory=%d/%d",
		health.GetRuntime().GetConnected(),
		health.GetRuntime().GetStarted(),
		metrics.GetCpuCores(),
		metrics.GetCpuUsagePercent(),
		metrics.GetMemoryUsed(),
		metrics.GetMemoryTotal(),
	)
}

func TestGRPCServerAcceptsPinnedTLSWithoutClientCertificate(t *testing.T) {
	tempDir := t.TempDir()
	serverCertFile, serverKeyFile := writeSelfSignedCert(t, tempDir, "server", []string{"rebecca-node.test"})

	settings := appconfig.Settings{
		AppName:     "rebecca-node",
		InstallMode: "binary",
		NodeVersion: "0.2.2",
		SSLCertFile: serverCertFile,
		SSLKeyFile:  serverKeyFile,
	}
	tlsConfig, err := loadGRPCServerTLS(settings)
	if err != nil {
		t.Fatalf("failed to load gRPC TLS config: %v", err)
	}
	if tlsConfig.ClientAuth != tls.NoClientCert {
		t.Fatalf("expected legacy-compatible no-client-cert mode, got %v", tlsConfig.ClientAuth)
	}

	server := &Server{
		settings: settings,
		core:     &xray.Core{},
		usage:    newUsageBuffer(),
		system:   newSystemSampler(),
		sessions: make(map[string]time.Time),
	}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	server.registerGRPC(grpcServer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("gRPC test server stopped: %v", err)
		}
	}()
	defer grpcServer.Stop()

	serverRootPEM, err := os.ReadFile(serverCertFile)
	if err != nil {
		t.Fatalf("failed to read server cert: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(serverRootPEM) {
		t.Fatal("failed to add server cert root")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		listener.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			ServerName: "rebecca-node.test",
			RootCAs:    roots,
			MinVersion: tls.VersionTLS12,
		})),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("failed to dial gRPC server without client cert: %v", err)
	}
	defer conn.Close()

	control := nodev1.NewNodeControlServiceClient(conn)
	hello, err := control.Hello(ctx, &nodev1.HelloRequest{MasterId: "legacy-master"})
	if err != nil {
		t.Fatalf("hello failed: %v", err)
	}
	if hello.GetNodeVersion() != "0.2.2" || hello.GetInstallMode() != "binary" {
		t.Fatalf("unexpected hello response: %#v", hello)
	}
}

func TestGRPCNodeVersionPrefersBinaryMetadata(t *testing.T) {
	tempDir := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(tempDir, ".binary-release.json"),
		[]byte(`{"install_mode":"binary","tag":"dev-abcdef0","arch":"linux-amd64"}`),
		0o600,
	); err != nil {
		t.Fatalf("failed to write metadata: %v", err)
	}
	server := &Server{
		settings: appconfig.Settings{
			AppName:        "rebecca-node",
			InstallMode:    "binary",
			NodeVersion:    "0.2.2",
			RebeccaDataDir: tempDir,
		},
		core:     &xray.Core{},
		usage:    newUsageBuffer(),
		sessions: make(map[string]time.Time),
	}
	api := &grpcAPI{server: server}

	hello, err := api.Hello(context.Background(), &nodev1.HelloRequest{})
	if err != nil {
		t.Fatalf("hello failed: %v", err)
	}
	if hello.GetNodeVersion() != "dev-abcdef0" {
		t.Fatalf("expected metadata node version, got %q", hello.GetNodeVersion())
	}
	if hello.GetUpdateChannel() != "dev" {
		t.Fatalf("expected dev update channel, got %q", hello.GetUpdateChannel())
	}
	if hello.GetRuntime().GetNodeVersion() != "dev-abcdef0" {
		t.Fatalf("expected runtime metadata node version, got %q", hello.GetRuntime().GetNodeVersion())
	}
}

func TestGRPCTestOutboundRejectsMissingTag(t *testing.T) {
	api := &grpcAPI{server: &Server{core: &xray.Core{}}}
	response, err := api.TestOutbound(context.Background(), &nodev1.OutboundTestRequest{
		AllOutboundsJson: `[{"tag":"direct","protocol":"freedom"}]`,
	})
	if err != nil {
		t.Fatalf("test outbound failed: %v", err)
	}
	if response.GetSuccess() {
		t.Fatalf("expected unsuccessful outbound test")
	}
	if response.GetError() != "Outbound has no tag" {
		t.Fatalf("unexpected outbound test error: %q", response.GetError())
	}
}

func TestReplaceMarkedBlock(t *testing.T) {
	current := "Keep 1\n# BEGIN REBECCA TOR PROXY\nold\n# END REBECCA TOR PROXY\nKeep 2\n"
	got := replaceMarkedBlock(current, torRebeccaBlockStart, torRebeccaBlockEnd, "new")
	if got != "Keep 1\n\nKeep 2\n\nnew\n" {
		t.Fatalf("replaceMarkedBlock()=%q", got)
	}
}

func TestPublicIPValidation(t *testing.T) {
	if !isGlobalIP("8.8.8.8", true) {
		t.Fatal("expected 8.8.8.8 to be a global IPv4")
	}
	if isGlobalIP("10.0.0.1", true) {
		t.Fatal("expected private IPv4 to be rejected")
	}
	if !isGlobalIP("2001:4860:4860::8888", false) {
		t.Fatal("expected Google DNS IPv6 to be a global IPv6")
	}
	if isGlobalIP("::1", false) {
		t.Fatal("expected loopback IPv6 to be rejected")
	}
}

func TestCanonicalConfigTopologyKeepsReverseClients(t *testing.T) {
	base := `{"inbounds":[{"tag":"vless","settings":{"clients":[{"id":"user-1","email":"user"}]}}]}`
	withUser := `{"inbounds":[{"tag":"vless","settings":{"clients":[{"id":"user-2","email":"other"}]}}]}`
	withReverse := `{"inbounds":[{"tag":"vless","settings":{"clients":[{"id":"user-1","email":"user"},{"id":"reverse-1","reverse":{"tag":"reverse-out"}}]}}]}`

	baseTopology, ok := canonicalConfigTopologyJSON(base)
	if !ok {
		t.Fatal("failed to normalize base topology")
	}
	userTopology, ok := canonicalConfigTopologyJSON(withUser)
	if !ok || userTopology != baseTopology {
		t.Fatalf("ordinary users must not change topology: %q != %q", userTopology, baseTopology)
	}
	reverseTopology, ok := canonicalConfigTopologyJSON(withReverse)
	if !ok || reverseTopology == baseTopology {
		t.Fatal("reverse clients must trigger a topology update")
	}
}

func writeSelfSignedCert(t *testing.T, dir string, name string, dnsNames []string) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("failed to generate serial: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:         true,
		DNSNames:     dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create cert: %v", err)
	}

	certFile := filepath.Join(dir, name+".pem")
	keyFile := filepath.Join(dir, name+"-key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("failed to write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("failed to write key: %v", err)
	}
	return certFile, keyFile
}
