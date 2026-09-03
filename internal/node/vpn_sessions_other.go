//go:build !unix

package node

func withVPNFileLock(_ string, fn func()) error {
	fn()
	return nil
}
