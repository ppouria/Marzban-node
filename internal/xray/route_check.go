package xray

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	routercommand "github.com/xtls/xray-core/app/router/command"
	xnet "github.com/xtls/xray-core/common/net"
)

type RouteTestRequest struct {
	InboundTag string
	Domain     string
	IP         string
	Port       uint32
	Network    string
	Protocol   string
	Email      string
}

type RouteTestResult struct {
	Matched     bool
	OutboundTag string
	GroupTags   []string
}

func TestRoute(apiHost string, apiPort int, timeout time.Duration, req RouteTestRequest) (RouteTestResult, error) {
	domain := strings.TrimSpace(req.Domain)
	ip := strings.TrimSpace(req.IP)
	if domain == "" && ip == "" {
		return RouteTestResult{}, fmt.Errorf("domain or ip is required")
	}

	network := xnet.Network_TCP
	if strings.EqualFold(req.Network, "udp") {
		network = xnet.Network_UDP
	}
	routingContext := &routercommand.RoutingContext{
		InboundTag:   strings.TrimSpace(req.InboundTag),
		Network:      network,
		TargetDomain: domain,
		TargetPort:   req.Port,
		Protocol:     strings.TrimSpace(req.Protocol),
		User:         strings.TrimSpace(req.Email),
	}
	if ip != "" {
		parsed := net.ParseIP(ip)
		if parsed == nil {
			return RouteTestResult{}, fmt.Errorf("invalid ip address: %s", ip)
		}
		if v4 := parsed.To4(); v4 != nil {
			routingContext.TargetIPs = [][]byte{v4}
		} else {
			routingContext.TargetIPs = [][]byte{parsed.To16()}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := dialAPI(ctx, apiHost, apiPort)
	if err != nil {
		return RouteTestResult{}, fmt.Errorf("connect to Xray routing API: %w", err)
	}
	defer conn.Close()

	resp, err := routercommand.NewRoutingServiceClient(conn).TestRoute(ctx, &routercommand.TestRouteRequest{
		RoutingContext: routingContext,
		PublishResult:  false,
	})
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not enough information") {
			return RouteTestResult{Matched: false}, nil
		}
		return RouteTestResult{}, fmt.Errorf("test Xray route: %w", err)
	}
	return RouteTestResult{
		Matched:     true,
		OutboundTag: resp.GetOutboundTag(),
		GroupTags:   resp.GetOutboundGroupTags(),
	}, nil
}
