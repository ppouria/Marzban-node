//go:build !linux

package node

import (
	"errors"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

type greLearner struct{}

func newGRELearner() *greLearner { return &greLearner{} }

func (m *extraVPNManager) Apply(runtimeConfig *extraRuntime) error {
	if runtimeConfig == nil {
		return nil
	}
	for _, inbound := range runtimeConfig.Inbounds {
		switch inbound.Protocol {
		case "sstp", "amneziawg", "gre":
			return errors.New("SSTP, AmneziaWG and GRE are supported only on Linux nodes")
		}
	}
	m.runtime = runtimeConfig
	return nil
}

func (m *extraVPNManager) collectUsageLocked() []xray.UserStat { return nil }
func runningGREInbounds(*extraRuntime) int                     { return 0 }
