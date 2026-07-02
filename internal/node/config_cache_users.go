package node

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

func (s *Server) addUserToConfigCache(inboundTag string, user xray.InboundUser) error {
	return s.patchConfigCacheUser(inboundTag, user, "")
}

func (s *Server) removeUserFromConfigCache(inboundTag string, email string) error {
	return s.patchConfigCacheUser(inboundTag, xray.InboundUser{}, email)
}

func (s *Server) patchConfigCacheUser(inboundTag string, user xray.InboundUser, removeEmail string) error {
	inboundTag = strings.TrimSpace(inboundTag)
	if inboundTag == "" {
		return fmt.Errorf("inbound_tag is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	payload, ok := s.loadConfigCache()
	if !ok {
		return nil
	}
	configJSON, changed, err := patchConfigCacheUserJSON(payload.Config, inboundTag, user, removeEmail)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	s.saveConfigCache(configJSON, payload.PeerIP)
	return nil
}

func patchConfigCacheUserJSON(rawConfig string, inboundTag string, user xray.InboundUser, removeEmail string) (string, bool, error) {
	var config map[string]any
	if err := json.Unmarshal([]byte(rawConfig), &config); err != nil {
		return "", false, err
	}
	inbounds, ok := config["inbounds"].([]any)
	if !ok {
		return "", false, nil
	}

	email := strings.TrimSpace(removeEmail)
	adding := email == ""
	if adding {
		email = strings.TrimSpace(user.Email)
	}
	if email == "" {
		return "", false, fmt.Errorf("email is required")
	}

	changed := false
	for _, item := range inbounds {
		inbound, ok := item.(map[string]any)
		if !ok || strings.TrimSpace(asString(inbound["tag"])) != inboundTag {
			continue
		}
		settings, ok := inbound["settings"].(map[string]any)
		if !ok {
			settings = map[string]any{}
			inbound["settings"] = settings
		}
		clients := anySlice(settings["clients"])
		next := make([]any, 0, len(clients)+1)
		for _, client := range clients {
			if strings.TrimSpace(clientEmail(client)) == email {
				changed = true
				continue
			}
			next = append(next, client)
		}
		if adding {
			next = append(next, inboundUserClientMap(user))
			changed = true
		}
		settings["clients"] = next
		break
	}
	if !changed {
		return "", false, nil
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return "", false, err
	}
	return string(encoded), true, nil
}

func inboundUserClientMap(user xray.InboundUser) map[string]any {
	client := map[string]any{
		"email": strings.TrimSpace(user.Email),
	}
	if user.Level != 0 {
		client["level"] = user.Level
	}
	if value := strings.TrimSpace(user.ID); value != "" {
		client["id"] = value
	}
	if value := strings.TrimSpace(user.Password); value != "" {
		client["password"] = value
	}
	if value := strings.TrimSpace(user.Auth); value != "" {
		client["auth"] = value
	}
	if value := strings.TrimSpace(user.Flow); value != "" {
		client["flow"] = value
	}
	if value := strings.TrimSpace(user.Method); value != "" {
		client["method"] = value
	}
	if user.CipherType != 0 {
		client["cipher_type"] = user.CipherType
	}
	if user.IVCheck {
		client["iv_check"] = true
	}
	return client
}

func clientEmail(value any) string {
	client, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	return asString(client["email"])
}

func anySlice(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case []map[string]any:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, item)
		}
		return result
	default:
		return nil
	}
}

func asString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case nil:
		return ""
	default:
		return fmt.Sprint(value)
	}
}
