package geecache

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func loggingUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	start := time.Now()
	resp, err = handler(ctx, req)
	code := status.Code(err)
	log.Printf("[grpc] method=%s code=%s duration=%s", info.FullMethod, code.String(), time.Since(start))
	return resp, err
}

func recoveryUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("[grpc] panic method=%s recovered=%v", info.FullMethod, recovered)
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	return handler(ctx, req)
}
