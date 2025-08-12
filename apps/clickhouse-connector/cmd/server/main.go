package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"

	"api-retailers-nest/apps/clickhouse-connector/internal/clickhouse"
	grpcserver "api-retailers-nest/apps/clickhouse-connector/internal/grpc"
)

func main() {
	// Configurer le logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Charger le fichier .env si présent
	if err := godotenv.Load(); err != nil {
		log.Printf("No .env file found: %v", err)
	}

	// Charger la configuration ClickHouse
	var clickhouseConfig clickhouse.Config
	if err := envconfig.Process("", &clickhouseConfig); err != nil {
		log.Fatalf("Failed to load ClickHouse config: %v", err)
		os.Exit(1)
	}

	// Charger la configuration gRPC
	var grpcConfig grpcserver.ServerConfig
	if err := envconfig.Process("", &grpcConfig); err != nil {
		log.Fatalf("Failed to load gRPC config: %v", err)
		os.Exit(1)
	}

	// Logger la configuration (sans les mots de passe)
	log.Printf("ClickHouse config: %s", clickhouseConfig.ConnectionString())
	log.Printf("gRPC config: Host=%s:%d, MaxRecvMsgSize=%d, MaxSendMsgSize=%d, Reflection=%v, HealthCheck=%v",
		grpcConfig.Host, grpcConfig.Port,
		grpcConfig.MaxRecvMsgSize, grpcConfig.MaxSendMsgSize,
		grpcConfig.EnableReflection, grpcConfig.EnableHealthCheck)

	// Créer le client ClickHouse
	log.Println("Connecting to ClickHouse...")
	clickhouseClient, err := clickhouse.NewClient(&clickhouseConfig)
	if err != nil {
		log.Fatalf("Failed to create ClickHouse client: %v", err)
		os.Exit(1)
	}
	defer func() {
		if err := clickhouseClient.Close(); err != nil {
			log.Printf("Error closing ClickHouse client: %v", err)
		}
	}()
	log.Println("Successfully connected to ClickHouse")

	// Créer le serveur gRPC
	log.Println("Creating gRPC server...")
	grpcServer, err := grpcserver.NewServer(&grpcConfig, clickhouseClient)
	if err != nil {
		log.Fatalf("Failed to create gRPC server: %v", err)
		os.Exit(1)
	}

	// Démarrer le serveur gRPC
	log.Println("Starting gRPC server...")
	if err := grpcServer.Start(); err != nil {
		log.Fatalf("gRPC server error: %v", err)
		os.Exit(1)
	}
}
