package geecache

import (
	"context"
	"geecache/internal/logx"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func loggingUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	start := time.Now()
	resp, err = handler(ctx, req)
	code := status.Code(err)
	logx.Event("grpc", "request", map[string]interface{}{
		"code":     code.String(),
		"duration": time.Since(start),
		"method":   info.FullMethod,
	})
	return resp, err
}

func recoveryUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logx.Event("grpc", "panic", map[string]interface{}{
				"method":    info.FullMethod,
				"recovered": recovered,
			})
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	return handler(ctx, req)
}
