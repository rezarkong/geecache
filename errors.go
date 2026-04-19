package geecache

import (
	"errors"
	"net"
)

var ErrNotFound = errors.New("[GCache]: key not found")

var ErrGroupNotFound = errors.New("[GCache]: group not found")

var ErrGroupClosed = errors.New("[GCache]: group is closed")

var ErrPeerViewMismatch = errors.New("[GCache]: peer view mismatch")

var ErrCircuitOpen = errors.New("[GCache]: peer circuit open")

func isRetryableError(err error) bool {
	if err == nil ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrGroupNotFound) ||
		errors.Is(err, ErrGroupClosed) ||
		errors.Is(err, ErrPeerViewMismatch) ||
		errors.Is(err, ErrCircuitOpen) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}

	return true
}
