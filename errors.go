package geecache

import (
	"errors"
	"net"
)

var ErrNotFound = errors.New("[GCache]: key not found")

var ErrGroupNotFound = errors.New("[GCache]: group not found")

var ErrCircuitOpen = errors.New("[GCache]: peer circuit open")

func isRetryableError(err error) bool {
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrGroupNotFound) || errors.Is(err, ErrCircuitOpen) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}

	return true
}
