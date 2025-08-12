package grpc

import (
	"context"
	"log"
	"strings"
	"time"

	"api-retailers-nest/apps/clickhouse-connector/internal/clickhouse"
	pb "api-retailers-nest/packages/proto/go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Handler implémente le service gRPC ClickHouse
type Handler struct {
	pb.UnimplementedClickHouseServiceServer
	client    *clickhouse.Client
	startTime time.Time
}

// NewHandler crée un nouveau handler gRPC
func NewHandler(client *clickhouse.Client) *Handler {
	return &Handler{
		client:    client,
		startTime: time.Now(),
	}
}

// HealthCheck vérifie l'état du service
func (h *Handler) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	uptime := time.Since(h.startTime).Seconds()

	// Tester la connexion ClickHouse
	err := h.client.TestConnection(ctx)
	if err != nil {
		log.Printf("Health check failed: %v", err)
		return &pb.HealthCheckResponse{
			Healthy:       false,
			Version:       "0.1.0",
			UptimeSeconds: int64(uptime),
			Error:         err.Error(),
		}, nil
	}

	return &pb.HealthCheckResponse{
		Healthy:       true,
		Version:       "0.1.0",
		UptimeSeconds: int64(uptime),
	}, nil
}

// GetRetailers exécute une requête SQL et convertit en Retailers
func (h *Handler) GetRetailers(ctx context.Context, req *pb.GetRetailersRequest) (*pb.GetRetailersResponse, error) {
	// Validation de base
	if req.SqlQuery == "" {
		return nil, status.Error(codes.InvalidArgument, "sql_query cannot be empty")
	}

	// Validation des paramètres de pagination
	if req.Limit < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit cannot be negative")
	}
	if req.Offset < 0 {
		return nil, status.Error(codes.InvalidArgument, "offset cannot be negative")
	}

	// Limite maximale de sécurité
	if req.Limit > 10000 {
		req.Limit = 10000
		log.Printf("Limit capped to 10000 for safety")
	}

	log.Printf("Executing SQL query: %s (limit=%d, offset=%d)",
		truncateString(req.SqlQuery, 200), req.Limit, req.Offset)

	// Exécuter la requête via le client générique
	result, executionTime, err := h.client.ExecuteQuery(ctx, req.SqlQuery, req.Parameters, req.Limit, req.Offset)
	if err != nil {
		log.Printf("Failed to execute query: %v", err)
		return &pb.GetRetailersResponse{
			Error: err.Error(),
		}, nil
	}

	// Convertir les résultats SQL en Retailers
	retailers := h.convertToRetailers(result)

	// S'assurer que retailers n'est jamais nil (pour forcer l'affichage en JSON)
	if retailers == nil {
		retailers = []*pb.Retailer{}
	}

	log.Printf("Successfully executed query: %d retailers returned in %dms", len(retailers), executionTime)
	return &pb.GetRetailersResponse{
		Retailers:       retailers,
		Count:           int64(len(retailers)),
		ExecutionTimeMs: executionTime,
	}, nil
}

// convertToRetailers convertit les résultats SQL bruts en types Retailer protobuf
func (h *Handler) convertToRetailers(result *clickhouse.SQLResult) []*pb.Retailer {
	var retailers []*pb.Retailer

	for _, row := range result.Rows {
		retailer := &pb.Retailer{}

		// Mapper les colonnes aux champs protobuf
		for columnName, value := range row {
			if value == nil {
				continue
			}

			columnNameLower := strings.ToLower(columnName)

			switch columnNameLower {
			case "id":
				if strValue, ok := value.(string); ok {
					retailer.Id = &strValue
				}
			case "name":
				if strValue, ok := value.(string); ok {
					retailer.Name = &strValue
				}
			case "created_at":
				if timeValue, ok := value.(time.Time); ok {
					retailer.CreatedAt = timestamppb.New(timeValue)
				}
			}
		}

		retailers = append(retailers, retailer)
	}

	return retailers
}

// GetRetailersStream exécute une requête SQL en streaming
func (h *Handler) GetRetailersStream(req *pb.GetRetailersRequest, stream pb.ClickHouseService_GetRetailersStreamServer) error {
	// Pour le streaming, on utilise la méthode non-streaming et on streame les résultats
	resp, err := h.GetRetailers(stream.Context(), req)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return status.Errorf(codes.Internal, "%s", resp.Error)
	}

	// Envoyer chaque retailer individuellement
	for _, retailer := range resp.Retailers {
		response := &pb.RetailerStreamResponse{
			Retailer: retailer,
			IsLast:   false,
		}

		if err := stream.Send(response); err != nil {
			return err
		}
	}

	// Envoyer le marqueur de fin avec le temps d'exécution
	endResponse := &pb.RetailerStreamResponse{
		IsLast:          true,
		ExecutionTimeMs: resp.ExecutionTimeMs,
	}

	log.Printf("Retailers stream completed successfully in %dms", resp.ExecutionTimeMs)
	return stream.Send(endResponse)
}

// truncateString tronque une chaîne pour le logging
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
