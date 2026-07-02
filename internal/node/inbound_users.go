package node

import (
	"net/http"
	"strings"
	"time"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

type addInboundUserPayload struct {
	SessionID  string           `json:"session_id"`
	InboundTag string           `json:"inbound_tag"`
	User       xray.InboundUser `json:"user"`
}

type removeInboundUserPayload struct {
	SessionID  string `json:"session_id"`
	InboundTag string `json:"inbound_tag"`
	Email      string `json:"email"`
}

func (s *Server) handleAddInboundUser(w http.ResponseWriter, r *http.Request) {
	var payload addInboundUserPayload
	if !decodeJSON(w, r, &payload) {
		return
	}
	if !s.matchSession(w, payload.SessionID) {
		return
	}
	if !s.core.Started() {
		writeError(w, http.StatusServiceUnavailable, "Xray is not started")
		return
	}

	payload.InboundTag = strings.TrimSpace(payload.InboundTag)
	if payload.InboundTag == "" {
		writeError(w, http.StatusUnprocessableEntity, "inbound_tag is required")
		return
	}

	if err := xray.AddInboundUser(
		s.settings.XrayAPIHost,
		s.settings.XrayAPIPort,
		60*time.Second,
		payload.InboundTag,
		payload.User,
	); err != nil {
		if !isIgnorableXrayAddError(err) {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	if err := s.addUserToConfigCache(payload.InboundTag, payload.User); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"status": "added"})
}

func (s *Server) handleRemoveInboundUser(w http.ResponseWriter, r *http.Request) {
	var payload removeInboundUserPayload
	if !decodeJSON(w, r, &payload) {
		return
	}
	if !s.matchSession(w, payload.SessionID) {
		return
	}
	if !s.core.Started() {
		writeError(w, http.StatusServiceUnavailable, "Xray is not started")
		return
	}

	payload.InboundTag = strings.TrimSpace(payload.InboundTag)
	payload.Email = strings.TrimSpace(payload.Email)
	if payload.InboundTag == "" {
		writeError(w, http.StatusUnprocessableEntity, "inbound_tag is required")
		return
	}
	if payload.Email == "" {
		writeError(w, http.StatusUnprocessableEntity, "email is required")
		return
	}

	if err := xray.RemoveInboundUser(
		s.settings.XrayAPIHost,
		s.settings.XrayAPIPort,
		60*time.Second,
		payload.InboundTag,
		payload.Email,
	); err != nil {
		if !isIgnorableXrayRemoveError(err) {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	if err := s.removeUserFromConfigCache(payload.InboundTag, payload.Email); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"status": "removed"})
}

func isIgnorableXrayRemoveError(err error) bool {
	if err == nil {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not found") ||
		strings.Contains(message, "not exist") ||
		strings.Contains(message, "no such user") ||
		strings.Contains(message, "email not found")
}

func isIgnorableXrayAddError(err error) bool {
	if err == nil {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already exists") ||
		strings.Contains(message, "email exists") ||
		strings.Contains(message, "duplicate")
}
