package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	nodev1 "github.com/rebeccapanel/rebecca-node/internal/proto/node/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type launched struct {
	cmd    *exec.Cmd
	output bytes.Buffer
}

func main() {
	nodeBinary := requireBinary("rebecca-node")

	tempDir, err := os.MkdirTemp("", "rebecca-node-binary-smoke-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(tempDir)

	if err := smokeNode(nodeBinary, filepath.Join(tempDir, "node")); err != nil {
		fatal(err)
	}

	fmt.Println("[smoke-test] Rebecca-node binaries passed smoke tests")
}

func smokeNode(binaryPath string, tempDir string) error {
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return err
	}
	fakeXray, err := createFakeXray(tempDir)
	if err != nil {
		return err
	}

	certPath := filepath.Join(tempDir, "ssl_cert.pem")
	keyPath := filepath.Join(tempDir, "ssl_key.pem")
	port := "43110"
	env := append(os.Environ(),
		"SERVICE_HOST=127.0.0.1",
		"SERVICE_PORT="+port,
		"XRAY_EXECUTABLE_PATH="+fakeXray,
		"XRAY_ASSETS_PATH="+tempDir,
		"SSL_CERT_FILE="+certPath,
		"SSL_KEY_FILE="+keyPath,
		"SSL_CLIENT_CERT_FILE="+certPath,
		"NODE_VERSION=binary-smoke-test",
	)
	process, err := launch(binaryPath, env)
	if err != nil {
		return err
	}
	defer terminate(process)

	if err := waitUntil(func() bool {
		return fileExists(certPath) && fileExists(keyPath)
	}, 20*time.Second, process, "Node binary did not generate certificates"); err != nil {
		return err
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return err
	}
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		return fmt.Errorf("failed to load generated node certificate")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		"127.0.0.1:"+port,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			ServerName:   "localhost",
			RootCAs:      roots,
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("node binary did not accept gRPC TLS connections: %w. Last output: %s", err, process.output.String())
	}
	defer conn.Close()

	control := nodev1.NewNodeControlServiceClient(conn)
	hello, err := control.Hello(ctx, &nodev1.HelloRequest{MasterId: "binary-smoke-test"})
	if err != nil {
		return fmt.Errorf("gRPC hello failed: %w", err)
	}
	if hello.GetNodeVersion() != "binary-smoke-test" {
		return fmt.Errorf("unexpected node version %q", hello.GetNodeVersion())
	}
	connected, err := control.Connect(ctx, &nodev1.ConnectRequest{MasterId: "binary-smoke-test"})
	if err != nil {
		return fmt.Errorf("gRPC connect failed: %w", err)
	}
	if connected.GetConnectionId() == "" || !connected.GetRuntime().GetConnected() {
		return fmt.Errorf("unexpected gRPC connect response: %#v", connected)
	}
	health, err := control.Health(ctx, &nodev1.HealthRequest{})
	if err != nil {
		return fmt.Errorf("gRPC health failed: %w", err)
	}
	if !health.GetRuntime().GetConnected() {
		return fmt.Errorf("gRPC health did not retain the connected session")
	}
	return nil
}

func createFakeXray(dir string) (string, error) {
	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "fake-xray.cmd")
		content := "@echo off\r\nif \"%1\"==\"version\" (\r\n  echo Xray 1.8.24\r\n  exit /b 0\r\n)\r\nif \"%1\"==\"run\" (\r\n  powershell -NoProfile -Command \"Start-Sleep -Seconds 60\"\r\n  exit /b 0\r\n)\r\nexit /b 0\r\n"
		return path, os.WriteFile(path, []byte(content), 0o755)
	}
	path := filepath.Join(dir, "fake-xray")
	content := "#!/usr/bin/env bash\nif [ \"$1\" = \"version\" ]; then echo 'Xray 1.8.24'; exit 0; fi\nif [ \"$1\" = \"run\" ]; then sleep 60; exit 0; fi\nexit 0\n"
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		return "", err
	}
	return path, nil
}

func launch(binaryPath string, env []string) (*launched, error) {
	process := &launched{cmd: exec.Command(binaryPath)}
	process.cmd.Env = env
	process.cmd.Stdout = &process.output
	process.cmd.Stderr = &process.output
	if err := process.cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		_ = process.cmd.Wait()
	}()
	return process, nil
}

func terminate(process *launched) {
	if process == nil || process.cmd == nil || process.cmd.Process == nil || process.cmd.ProcessState != nil {
		return
	}
	if runtime.GOOS == "windows" {
		_ = exec.Command("taskkill", "/PID", fmt.Sprint(process.cmd.Process.Pid), "/T", "/F").Run()
		return
	}
	_ = process.cmd.Process.Kill()
}

func waitUntil(predicate func() bool, timeout time.Duration, process *launched, message string) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if process.cmd.ProcessState != nil {
			return fmt.Errorf("%s. Process exited early: %s", message, process.output.String())
		}
		if predicate() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("%s. Last output: %s", message, process.output.String())
}

func requireBinary(name string) string {
	path := filepath.Join("dist", name+suffix())
	if !fileExists(path) {
		fatal(fmt.Errorf("required binary is missing: %s", path))
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		fatal(err)
	}
	return abs
}

func suffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
