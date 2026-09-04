package node

type extraRuntime struct {
	GeneratedAt     string                `json:"generated_at"`
	Target          string                `json:"target,omitempty"`
	SessionCallback *vpnSessionCallback   `json:"session_callback,omitempty"`
	Inbounds        []extraRuntimeInbound `json:"inbounds"`
}

type extraRuntimeInbound struct {
	Tag        string             `json:"tag"`
	Protocol   string             `json:"protocol"`
	Listen     string             `json:"listen"`
	Port       int                `json:"port"`
	TunnelTag  string             `json:"tunnel_tag,omitempty"`
	TunnelPort int                `json:"tunnel_port,omitempty"`
	Settings   map[string]any     `json:"settings"`
	Users      []extraRuntimeUser `json:"users,omitempty"`
	Peers      []wgRuntimePeer    `json:"peers,omitempty"`
}

type extraRuntimeUser struct {
	UserID        int64    `json:"user_id"`
	Username      string   `json:"username"`
	Password      string   `json:"password,omitempty"`
	IPv4Address   string   `json:"ipv4_address,omitempty"`
	IPv4Addresses []string `json:"ipv4_addresses,omitempty"`
	Status        string   `json:"status,omitempty"`
	UsedTraffic   int64    `json:"used_traffic,omitempty"`
	DataLimit     *int64   `json:"data_limit,omitempty"`
	Expire        *int64   `json:"expire,omitempty"`
	DeviceLimit   int64    `json:"device_limit,omitempty"`
}
