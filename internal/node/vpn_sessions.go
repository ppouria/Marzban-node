package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type vpnSessionCallback struct {
	URL    string `json:"url,omitempty"`
	Token  string `json:"token,omitempty"`
	NodeID int64  `json:"node_id,omitempty"`
}

type vpnSessionEvent struct {
	NodeID     int64  `json:"node_id"`
	UserID     int64  `json:"user_id"`
	Protocol   string `json:"protocol"`
	InboundTag string `json:"inbound_tag,omitempty"`
	SessionID  string `json:"session_id"`
	AssignedIP string `json:"assigned_ip,omitempty"`
	ClientIP   string `json:"client_ip,omitempty"`
	Event      string `json:"event"`
}

var vpnSessionMu sync.Mutex

func vpnSessionsPath(baseDir string) string {
	return filepath.Join(filepath.Dir(baseDir), "vpn-sessions.tsv")
}

func vpnSessionCallbackPath(dir string) string {
	return filepath.Join(dir, "session-callback.env")
}

func writeVPNSessionCallback(path string, callback *vpnSessionCallback) error {
	body := "CALLBACK_URL=\nCALLBACK_TOKEN=\nCALLBACK_NODE_ID=\n"
	if callback != nil && strings.TrimSpace(callback.URL) != "" && strings.TrimSpace(callback.Token) != "" && callback.NodeID > 0 {
		body = "CALLBACK_URL=" + strconv.Quote(strings.TrimSpace(callback.URL)) + "\n" +
			"CALLBACK_TOKEN=" + strconv.Quote(strings.TrimSpace(callback.Token)) + "\n" +
			"CALLBACK_NODE_ID=" + strconv.FormatInt(callback.NodeID, 10) + "\n"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return writeFileIfChanged(path, []byte(body), 0o600)
}

func vpnSessionShell(callbackPath string, sessionsPath string) string {
	return fmt.Sprintf(`
VPN_CALLBACK=%q
VPN_SESSIONS=%q
VPN_LOCK="${VPN_SESSIONS}.lock"

vpn_safe() {
  printf '%%s' "$1" | tr -c 'A-Za-z0-9_.:-' '_'
}

vpn_device_key() {
  ip=$1
  client_ip=$2
  session=$3
  if [ -n "$client_ip" ]; then
    printf 'client:%%s' "$(vpn_safe "$client_ip")"
  elif [ -n "$ip" ]; then
    printf 'assigned:%%s' "$(vpn_safe "$ip")"
  else
    printf 'session:%%s' "$(vpn_safe "$session")"
  fi
}

vpn_notify() {
  event=$1
  uid=$2
  proto=$3
  tag=$4
  session=$5
  ip=$6
  client_ip=$7
  [ -f "$VPN_CALLBACK" ] && . "$VPN_CALLBACK"
  [ -n "$CALLBACK_URL" ] && [ -n "$CALLBACK_TOKEN" ] && [ -n "$CALLBACK_NODE_ID" ] || return 0
  command -v curl >/dev/null 2>&1 || return 0
  json=$(printf '{"node_id":%%s,"user_id":%%s,"protocol":"%%s","inbound_tag":"%%s","session_id":"%%s","assigned_ip":"%%s","client_ip":"%%s","event":"%%s"}' "$CALLBACK_NODE_ID" "$uid" "$(vpn_safe "$proto")" "$(vpn_safe "$tag")" "$(vpn_safe "$session")" "$(vpn_safe "$ip")" "$(vpn_safe "$client_ip")" "$event")
  curl -fsS -m 3 -H "Authorization: Bearer $CALLBACK_TOKEN" -H "Content-Type: application/json" --data "$json" "$CALLBACK_URL" >/dev/null 2>&1
}

vpn_admit() {
  uid=$1
  proto=$2
  tag=$3
  session=$4
  ip=$5
  client_ip=$6
  limit=$7
  [ -n "$uid" ] && [ -n "$session" ] || return 1
  case "$limit" in ''|*[!0-9]*) limit=0 ;; esac
  mkdir -p "$(dirname "$VPN_SESSIONS")"
  touch "$VPN_SESSIONS"
  (
    flock -x 9 || exit 50
    tmp="${VPN_SESSIONS}.$$"
    awk -F '\t' -v sid="$session" '$4 != sid { print }' "$VPN_SESSIONS" > "$tmp"
    device_key=$(vpn_device_key "$ip" "$client_ip" "$session")
    count=$(awk -F '\t' -v uid="$uid" '
      function key(ip, client_ip, session) {
        if (client_ip != "") return "client:" client_ip
        if (ip != "") return "assigned:" ip
        return "session:" session
      }
      $1 == uid {
        client = (NF >= 7 ? $6 : "")
        k = key($5, client, $4)
        if (!(k in seen)) { seen[k] = 1; n++ }
      }
      END { print n+0 }
    ' "$tmp")
    exists=$(awk -F '\t' -v uid="$uid" -v want="$device_key" '
      function key(ip, client_ip, session) {
        if (client_ip != "") return "client:" client_ip
        if (ip != "") return "assigned:" ip
        return "session:" session
      }
      $1 == uid {
        client = (NF >= 7 ? $6 : "")
        if (key($5, client, $4) == want) found = 1
      }
      END { print found+0 }
    ' "$tmp")
    ip_conflict=$(awk -F '\t' -v proto="$proto" -v tag="$tag" -v ip="$ip" '
      ip != "" && $2 == proto && $3 == tag && $5 == ip { found = 1 }
      END { print found+0 }
    ' "$tmp")
    if [ "$ip_conflict" -eq 1 ]; then
      rm -f "$tmp"
      exit 31
    fi
    if [ "$limit" -gt 0 ] && [ "$count" -ge "$limit" ] && [ "$exists" -eq 0 ]; then
      rm -f "$tmp"
      exit 30
    fi
    if [ "$limit" -gt 0 ] && ! vpn_notify start "$uid" "$proto" "$tag" "$session" "$ip" "$client_ip"; then
      rm -f "$tmp"
      exit 32
    fi
    printf '%%s\t%%s\t%%s\t%%s\t%%s\t%%s\t%%s\n' "$uid" "$proto" "$tag" "$session" "$ip" "$client_ip" "$(date +%%s)" >> "$tmp"
    mv "$tmp" "$VPN_SESSIONS"
    chmod 600 "$VPN_SESSIONS"
  ) 9>"$VPN_LOCK"
  rc=$?
  [ "$rc" -eq 0 ] && vpn_notify start "$uid" "$proto" "$tag" "$session" "$ip" "$client_ip"
  return "$rc"
}

vpn_release() {
  uid=$1
  proto=$2
  tag=$3
  session=$4
  ip=$5
  client_ip=$6
  [ -n "$uid" ] && [ -n "$session" ] || return 0
  mkdir -p "$(dirname "$VPN_SESSIONS")"
  touch "$VPN_SESSIONS"
  (
    flock -x 9 || exit 0
    tmp="${VPN_SESSIONS}.$$"
    awk -F '\t' -v sid="$session" '$4 != sid { print }' "$VPN_SESSIONS" > "$tmp"
    mv "$tmp" "$VPN_SESSIONS"
    chmod 600 "$VPN_SESSIONS"
  ) 9>"$VPN_LOCK"
  vpn_notify stop "$uid" "$proto" "$tag" "$session" "$ip" "$client_ip"
  return 0
}
`, callbackPath, sessionsPath)
}

func vpnAdmitGoSession(path string, callback *vpnSessionCallback, event vpnSessionEvent, limit int64) bool {
	if event.UserID <= 0 || strings.TrimSpace(event.SessionID) == "" {
		return false
	}
	sessionID := safeName(event.SessionID)
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	admitted := false
	preauthorized := false
	if err := withVPNSessionLock(path, func() {
		records := vpnSessionRecordsLocked(path)
		next := records[:0]
		for _, record := range records {
			if len(record) >= 4 && record[3] == sessionID {
				continue
			}
			next = append(next, record)
		}
		count := 0
		uidText := strconv.FormatInt(event.UserID, 10)
		deviceKey := vpnSessionDeviceKey(event.AssignedIP, event.ClientIP, sessionID)
		hasDevice := false
		assignedIPInUse := false
		devices := map[string]struct{}{}
		for _, record := range next {
			if len(record) >= 5 &&
				record[1] == normalizedVPNProtocol(event.Protocol) &&
				record[2] == safeName(event.InboundTag) &&
				record[4] != "" &&
				record[4] == strings.TrimSpace(event.AssignedIP) {
				assignedIPInUse = true
			}
			if len(record) >= 1 && record[0] == uidText {
				key := vpnRecordDeviceKey(record)
				if key == "" {
					continue
				}
				if key == deviceKey {
					hasDevice = true
				}
				if _, ok := devices[key]; !ok {
					devices[key] = struct{}{}
					count++
				}
			}
		}
		if assignedIPInUse {
			return
		}
		if limit > 0 && int64(count) >= limit && !hasDevice {
			return
		}
		if limit > 0 && vpnSessionCallbackReady(callback) {
			if !vpnSendSession(callback, event) {
				return
			}
			preauthorized = true
		}
		next = append(next, []string{
			uidText,
			normalizedVPNProtocol(event.Protocol),
			safeName(event.InboundTag),
			sessionID,
			strings.TrimSpace(event.AssignedIP),
			strings.TrimSpace(event.ClientIP),
			strconv.FormatInt(time.Now().Unix(), 10),
		})
		vpnWriteSessionRecordsLocked(path, next)
		admitted = true
	}); err != nil {
		return false
	}
	if admitted && !preauthorized {
		go vpnNotifySession(callback, event)
	}
	return admitted
}

func vpnRecordDeviceKey(record []string) string {
	if len(record) < 4 {
		return ""
	}
	assignedIP := ""
	if len(record) >= 5 {
		assignedIP = record[4]
	}
	clientIP := ""
	if len(record) >= 7 {
		clientIP = record[5]
	}
	return vpnSessionDeviceKey(assignedIP, clientIP, record[3])
}

func vpnSessionDeviceKey(assignedIP string, clientIP string, sessionID string) string {
	if text := strings.TrimSpace(clientIP); text != "" {
		return "client:" + safeName(text)
	}
	if text := strings.TrimSpace(assignedIP); text != "" {
		return "assigned:" + safeName(text)
	}
	if text := strings.TrimSpace(sessionID); text != "" {
		return "session:" + safeName(text)
	}
	return ""
}

func vpnReleaseGoSession(path string, callback *vpnSessionCallback, event vpnSessionEvent) {
	if event.UserID <= 0 || strings.TrimSpace(event.SessionID) == "" {
		return
	}
	sessionID := safeName(event.SessionID)
	_ = withVPNSessionLock(path, func() {
		records := vpnSessionRecordsLocked(path)
		next := records[:0]
		for _, record := range records {
			if len(record) >= 4 && record[3] == sessionID {
				continue
			}
			next = append(next, record)
		}
		vpnWriteSessionRecordsLocked(path, next)
	})
	go vpnNotifySession(callback, event)
}

func vpnUserCanOpenSession(path string, userID int64, limit int64) bool {
	if userID <= 0 || limit <= 0 {
		return true
	}
	count := 0
	if err := withVPNSessionLock(path, func() {
		uidText := strconv.FormatInt(userID, 10)
		devices := map[string]struct{}{}
		for _, record := range vpnSessionRecordsLocked(path) {
			if len(record) >= 1 && record[0] == uidText {
				key := vpnRecordDeviceKey(record)
				if key == "" {
					continue
				}
				if _, ok := devices[key]; !ok {
					devices[key] = struct{}{}
					count++
				}
			}
		}
	}); err != nil {
		return false
	}
	return int64(count) < limit
}

func withVPNSessionLock(path string, fn func()) error {
	vpnSessionMu.Lock()
	defer vpnSessionMu.Unlock()
	return withVPNFileLock(path+".lock", fn)
}

func vpnSessionRecordsLocked(path string) [][]string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	records := [][]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		records = append(records, strings.Split(line, "\t"))
	}
	return records
}

func vpnWriteSessionRecordsLocked(path string, records [][]string) {
	var b strings.Builder
	for _, record := range records {
		if len(record) == 0 {
			continue
		}
		b.WriteString(strings.Join(record, "\t"))
		b.WriteByte('\n')
	}
	_ = os.WriteFile(path, []byte(b.String()), 0o600)
}

func vpnNotifySession(callback *vpnSessionCallback, event vpnSessionEvent) {
	_ = vpnSendSession(callback, event)
}

func vpnSessionCallbackReady(callback *vpnSessionCallback) bool {
	return callback != nil && strings.TrimSpace(callback.URL) != "" && strings.TrimSpace(callback.Token) != "" && callback.NodeID > 0
}

func vpnSendSession(callback *vpnSessionCallback, event vpnSessionEvent) bool {
	if !vpnSessionCallbackReady(callback) {
		return true
	}
	event.NodeID = callback.NodeID
	event.Protocol = normalizedVPNProtocol(event.Protocol)
	event.InboundTag = safeName(event.InboundTag)
	event.SessionID = safeName(event.SessionID)
	body, err := json.Marshal(event)
	if err != nil {
		return false
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimSpace(callback.URL), bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(callback.Token))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return false
	}
	_ = res.Body.Close()
	return res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices
}

func normalizedVPNProtocol(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "openvpn":
		return "ov"
	case "wireguard":
		return "wg"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}
