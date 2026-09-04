package node

import (
	"net"
	"strconv"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestSSHProxyRejectsShellAndPrivateDestinations(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	manager := newSSHProxyManager(t.TempDir())
	runtimeConfig := &extraRuntime{Inbounds: []extraRuntimeInbound{{
		Tag: "ssh-test", Protocol: "ssh", Listen: "127.0.0.1", Port: port,
		Settings: map[string]any{"idle_timeout": float64(30)},
		Users:    []extraRuntimeUser{{UserID: 7, Username: "alice", Password: "secret"}},
	}}}
	if err := manager.Apply(runtimeConfig); err != nil {
		t.Fatal(err)
	}
	defer manager.Apply(&extraRuntime{})

	client, err := ssh.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), &ssh.ClientConfig{
		User: "alice", Auth: []ssh.AuthMethod{ssh.Password("secret")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), Timeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if session, err := client.NewSession(); err == nil {
		_ = session.Close()
		t.Fatal("session/shell channel was accepted")
	}
	if connection, err := client.Dial("tcp", "127.0.0.1:80"); err == nil {
		_ = connection.Close()
		t.Fatal("private destination was accepted")
	}
}

func TestPublicProxyIP(t *testing.T) {
	for value, want := range map[string]bool{
		"1.1.1.1": true, "2606:4700:4700::1111": true,
		"127.0.0.1": false, "10.0.0.1": false, "169.254.169.254": false, "::1": false, "fd00::1": false,
	} {
		if got := isPublicProxyIP(net.ParseIP(value)); got != want {
			t.Fatalf("isPublicProxyIP(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestSSHDeviceLimitCountsDistinctIPs(t *testing.T) {
	manager := newSSHProxyManager(t.TempDir())
	if !manager.acquire("ssh:7", "198.51.100.1", 1) || !manager.acquire("ssh:7", "198.51.100.1", 1) {
		t.Fatal("same client IP should be allowed to open multiple SSH connections")
	}
	if manager.acquire("ssh:7", "198.51.100.2", 1) {
		t.Fatal("a second client IP bypassed the device limit")
	}
	manager.release("ssh:7", "198.51.100.1")
	manager.release("ssh:7", "198.51.100.1")
	if !manager.acquire("ssh:7", "198.51.100.2", 1) {
		t.Fatal("released client IP still occupied the device slot")
	}
}
