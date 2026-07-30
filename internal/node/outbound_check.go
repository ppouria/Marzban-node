package node

import "net/http"

type outboundTestPayload struct {
	SessionID        string           `json:"session_id"`
	OutboundTag      string           `json:"outbound_tag"`
	OutboundProtocol string           `json:"outbound_protocol"`
	AllOutbounds     []map[string]any `json:"all_outbounds"`
	TestURL          string           `json:"test_url"`
	TestType         string           `json:"test_type"`
}

func (s *Server) handleOutboundTest(w http.ResponseWriter, r *http.Request) {
	var payload outboundTestPayload
	if !decodeJSON(w, r, &payload) {
		return
	}
	if !s.matchSession(w, payload.SessionID) {
		return
	}
	result := s.core.TestOutbound(
		r.Context(),
		payload.OutboundTag,
		payload.OutboundProtocol,
		payload.AllOutbounds,
		payload.TestURL,
		payload.TestType,
	)
	writeJSON(w, http.StatusOK, result)
}
