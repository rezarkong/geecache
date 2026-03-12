package geecache

import (
	"errors"
	"net"
)

var ErrNotFound = errors.New("geecache: key not found")

var ErrCircuitOpen = errors.New("geecache: peer circuit open")

func isRetryableError(err error) bool {
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrCircuitOpen) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}

	return true
}
