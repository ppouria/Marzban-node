package node

import (
	"strings"
)

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
