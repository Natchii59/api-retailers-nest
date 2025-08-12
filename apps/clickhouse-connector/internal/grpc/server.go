package grpc

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"api-retailers-nest/apps/clickhouse-connector/internal/clickhouse"
	pb "api-retailers-nest/packages/proto/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Server représente le serveur gRPC
type Server struct {
	config           *ServerConfig
	clickhouseClient *clickhouse.Client
	grpcServer       *grpc.Server
	listener         net.Listener
}

// NewServer crée un nouveau serveur gRPC
func NewServer(config *ServerConfig, clickhouseClient *clickhouse.Client) (*Server, error) {
	return &Server{
		config:           config,
		clickhouseClient: clickhouseClient,
	}, nil
}

// Start démarre le serveur gRPC
func (s *Server) Start() error {
	// Créer le listener
	address := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}
	s.listener = listener

	// Options du serveur gRPC
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.config.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(s.config.MaxSendMsgSize),
		grpc.UnaryInterceptor(unaryInterceptor),
		grpc.StreamInterceptor(streamInterceptor),
	}

	// Créer le serveur gRPC
	s.grpcServer = grpc.NewServer(opts...)

	// Enregistrer le service ClickHouse
	handler := NewHandler(s.clickhouseClient)
	pb.RegisterClickHouseServiceServer(s.grpcServer, handler)

	// Enregistrer le service de health check si activé
	if s.config.EnableHealthCheck {
		healthServer := health.NewServer()
		grpc_health_v1.RegisterHealthServer(s.grpcServer, healthServer)
		healthServer.SetServingStatus("clickhouse.ClickHouseService", grpc_health_v1.HealthCheckResponse_SERVING)
		log.Println("Health check service enabled")
	}

	// Activer la réflexion si configuré (utile pour le développement)
	if s.config.EnableReflection {
		reflection.Register(s.grpcServer)
		log.Println("gRPC reflection enabled")
	}

	log.Printf("Starting gRPC server on %s", address)

	// Démarrer le serveur dans une goroutine
	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Attendre les signaux d'arrêt
	s.waitForShutdown()

	return nil
}

// Stop arrête le serveur gRPC
func (s *Server) Stop() {
	log.Println("Shutting down gRPC server...")

	// Arrêter gracieusement le serveur
	s.grpcServer.GracefulStop()

	// Fermer le client ClickHouse
	if s.clickhouseClient != nil {
		if err := s.clickhouseClient.Close(); err != nil {
			log.Printf("Error closing ClickHouse client: %v", err)
		}
	}

	log.Println("Server shutdown complete")
}

// waitForShutdown attend les signaux d'arrêt
func (s *Server) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal: %v", sig)

	s.Stop()
}
