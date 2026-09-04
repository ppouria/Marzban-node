//go:build !linux

package node

import "errors"

type externalProxyManager struct{}

func newExternalProxyManager(string, func() string) *externalProxyManager {
	return &externalProxyManager{}
}

func (*externalProxyManager) Apply(runtimeConfig *extraRuntime) error {
	if runtimeConfig != nil {
		for _, inbound := range runtimeConfig.Inbounds {
			if inbound.Protocol == "mtproto" || inbound.Protocol == "web" {
				return errors.New("MTProxy and WEB proxy are available only on Linux nodes")
			}
		}
	}
	return nil
}

func (*externalProxyManager) State(string) (int, int) { return 0, 0 }
