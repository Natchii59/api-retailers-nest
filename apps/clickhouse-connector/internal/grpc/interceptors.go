package grpc

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// unaryInterceptor intercepte les appels unaires pour le logging et la gestion d'erreurs
func unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()

	// Log de la requête entrante
	log.Printf("gRPC Unary call: %s", info.FullMethod)

	// Appeler le handler
	resp, err := handler(ctx, req)

	// Log de la réponse
	duration := time.Since(start)
	if err != nil {
		log.Printf("gRPC Unary call: %s - Error: %v - Duration: %v",
			info.FullMethod, err, duration)
	} else {
		log.Printf("gRPC Unary call: %s - Success - Duration: %v",
			info.FullMethod, duration)
	}

	return resp, err
}

// streamInterceptor intercepte les appels streaming pour le logging
func streamInterceptor(
	srv interface{},
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()

	// Log de la requête streaming entrante
	log.Printf("gRPC Stream call: %s", info.FullMethod)

	// Wrapper pour le stream avec logging
	wrappedStream := &wrappedServerStream{
		ServerStream: stream,
		method:       info.FullMethod,
	}

	// Appeler le handler
	err := handler(srv, wrappedStream)

	// Log de la fin du stream
	duration := time.Since(start)
	if err != nil {
		log.Printf("gRPC Stream call: %s - Error: %v - Duration: %v",
			info.FullMethod, err, duration)
	} else {
		log.Printf("gRPC Stream call: %s - Success - Duration: %v",
			info.FullMethod, duration)
	}

	return err
}

// wrappedServerStream wrapper pour ajouter du logging aux streams
type wrappedServerStream struct {
	grpc.ServerStream
	method string
}

// SendMsg intercepte l'envoi de messages dans le stream
func (w *wrappedServerStream) SendMsg(m interface{}) error {
	err := w.ServerStream.SendMsg(m)
	if err != nil {
		log.Printf("gRPC Stream %s - SendMsg error: %v", w.method, err)
	}
	return err
}

// RecvMsg intercepte la réception de messages dans le stream
func (w *wrappedServerStream) RecvMsg(m interface{}) error {
	err := w.ServerStream.RecvMsg(m)
	if err != nil {
		// Ne pas logger les erreurs EOF qui sont normales
		if status.Code(err) != codes.OutOfRange {
			log.Printf("gRPC Stream %s - RecvMsg error: %v", w.method, err)
		}
	}
	return err
}

// recoveryInterceptor récupère les panics et les convertit en erreurs gRPC
func recoveryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("gRPC panic recovered in %s: %v", info.FullMethod, r)
			err = status.Errorf(codes.Internal, "internal server error")
		}
	}()

	return handler(ctx, req)
}

// streamRecoveryInterceptor récupère les panics dans les streams
func streamRecoveryInterceptor(
	srv interface{},
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("gRPC stream panic recovered in %s: %v", info.FullMethod, r)
			err = status.Errorf(codes.Internal, "internal server error")
		}
	}()

	return handler(srv, stream)
}
