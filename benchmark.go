package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	pb "api-retailers-nest/packages/proto/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BenchmarkConfig struct {
	ServerAddr  string
	NumRequests int
	Concurrency int
	TestQueries []TestQuery
}

type TestQuery struct {
	Name        string
	SqlQuery    string
	Parameters  map[string]string
	Limit       int32
	Description string
}

type BenchmarkResult struct {
	QueryName         string
	TotalRequests     int
	Concurrency       int
	TotalDuration     time.Duration
	AvgDuration       time.Duration
	MinDuration       time.Duration
	MaxDuration       time.Duration
	RequestsPerSecond float64
	Errors            int
	AvgServerTime     time.Duration
	AvgResultCount    float64
}

func main() {
	// Configuration du benchmark
	config := BenchmarkConfig{
		ServerAddr:  "localhost:50051",
		NumRequests: 100, // Réduit pour des tests plus rapides
		Concurrency: 5,   // Réduit aussi
		TestQueries: []TestQuery{
			{
				Name:        "simple_limit",
				SqlQuery:    "SELECT id, name FROM retailers",
				Limit:       10,
				Description: "Requête simple avec LIMIT",
			},
			{
				Name:        "filter_by_name",
				SqlQuery:    "SELECT * FROM retailers WHERE name LIKE ?",
				Parameters:  map[string]string{"1": "%Retailer%"},
				Limit:       50,
				Description: "Requête avec filtrage par nom",
			},
			{
				Name:        "large_result",
				SqlQuery:    "SELECT id, name FROM retailers",
				Limit:       1000,
				Description: "Requête avec résultat plus large",
			},
			{
				Name:        "date_filter",
				SqlQuery:    "SELECT * FROM retailers WHERE created_at > toDateTime(?)",
				Parameters:  map[string]string{"1": "2024-01-01 00:00:00"},
				Limit:       100,
				Description: "Requête avec filtre de date",
			},
			{
				Name:        "count_only",
				SqlQuery:    "SELECT COUNT(*) as count FROM retailers",
				Description: "Requête de comptage simple",
			},
			{
				Name:        "retailer_pattern",
				SqlQuery:    "SELECT id, name FROM retailers WHERE name LIKE ?",
				Parameters:  map[string]string{"1": "%Retailer%"},
				Limit:       100,
				Description: "Requête sur le pattern Retailer (devrait avoir des résultats)",
			},
		},
	}

	fmt.Println("🚀 ClickHouse gRPC Server Benchmark")
	fmt.Println("=====================================")
	fmt.Printf("Server: %s\n", config.ServerAddr)
	fmt.Printf("Requests per query: %d\n", config.NumRequests)
	fmt.Printf("Concurrency: %d\n", config.Concurrency)
	fmt.Println()

	// Connexion au serveur gRPC
	conn, err := grpc.Dial(config.ServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewClickHouseServiceClient(conn)

	// Test de sanité
	fmt.Println("🔍 Health Check...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	health, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}

	if !health.Healthy {
		log.Fatalf("Server is not healthy: %s", health.Error)
	}

	fmt.Printf("✅ Server healthy (uptime: %ds, version: %s)\n\n", health.UptimeSeconds, health.Version)

	// Exécuter les benchmarks
	var allResults []BenchmarkResult

	for _, query := range config.TestQueries {
		fmt.Printf("📊 Testing: %s - %s\n", query.Name, query.Description)

		result := runBenchmark(client, query, config.NumRequests, config.Concurrency)
		allResults = append(allResults, result)

		printResult(result)
		fmt.Println()
	}

	// Résumé global
	printSummary(allResults)
}

func runBenchmark(client pb.ClickHouseServiceClient, query TestQuery, numRequests, concurrency int) BenchmarkResult {
	var wg sync.WaitGroup
	var mu sync.Mutex

	durations := make([]time.Duration, 0, numRequests)
	serverTimes := make([]time.Duration, 0, numRequests)
	resultCounts := make([]int, 0, numRequests)
	errors := 0

	// Canal pour limiter la concurrence
	semaphore := make(chan struct{}, concurrency)

	start := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Acquérir le semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			reqStart := time.Now()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			resp, err := client.GetRetailers(ctx, &pb.GetRetailersRequest{
				SqlQuery:   query.SqlQuery,
				Parameters: query.Parameters,
				Limit:      query.Limit,
			})

			reqDuration := time.Since(reqStart)

			mu.Lock()
			durations = append(durations, reqDuration)

			if err != nil {
				errors++
			} else if resp.Error != "" {
				errors++
			} else {
				serverTimes = append(serverTimes, time.Duration(resp.ExecutionTimeMs)*time.Millisecond)
				resultCounts = append(resultCounts, int(resp.Count))
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	// Calculer les statistiques
	avgDuration := calculateAverage(durations)
	minDuration := calculateMin(durations)
	maxDuration := calculateMax(durations)
	avgServerTime := calculateAverage(serverTimes)
	avgResultCount := calculateAverageInt(resultCounts)
	rps := float64(numRequests) / totalDuration.Seconds()

	return BenchmarkResult{
		QueryName:         query.Name,
		TotalRequests:     numRequests,
		Concurrency:       concurrency,
		TotalDuration:     totalDuration,
		AvgDuration:       avgDuration,
		MinDuration:       minDuration,
		MaxDuration:       maxDuration,
		RequestsPerSecond: rps,
		Errors:            errors,
		AvgServerTime:     avgServerTime,
		AvgResultCount:    avgResultCount,
	}
}

func printResult(result BenchmarkResult) {
	fmt.Printf("  Total time: %v\n", result.TotalDuration)
	fmt.Printf("  Requests/sec: %.2f\n", result.RequestsPerSecond)
	fmt.Printf("  Avg latency: %v (server: %v)\n", result.AvgDuration, result.AvgServerTime)
	fmt.Printf("  Min/Max latency: %v / %v\n", result.MinDuration, result.MaxDuration)
	fmt.Printf("  Avg result count: %.1f\n", result.AvgResultCount)
	fmt.Printf("  Errors: %d/%d (%.2f%%)\n",
		result.Errors, result.TotalRequests,
		float64(result.Errors)/float64(result.TotalRequests)*100)
}

func printSummary(results []BenchmarkResult) {
	fmt.Println("📈 BENCHMARK SUMMARY")
	fmt.Println("===================")
	fmt.Printf("%-20s %10s %10s %15s %10s %10s\n",
		"Query", "RPS", "Avg Lat", "Server Time", "Results", "Errors")
	fmt.Println(strings.Repeat("-", 85))

	var totalRPS float64
	var totalErrors int
	var totalRequests int

	for _, result := range results {
		fmt.Printf("%-20s %10.1f %10v %15v %10.0f %9d%%\n",
			result.QueryName,
			result.RequestsPerSecond,
			result.AvgDuration,
			result.AvgServerTime,
			result.AvgResultCount,
			int(float64(result.Errors)/float64(result.TotalRequests)*100))

		totalRPS += result.RequestsPerSecond
		totalErrors += result.Errors
		totalRequests += result.TotalRequests
	}

	fmt.Println(strings.Repeat("-", 85))
	fmt.Printf("Total RPS: %.1f | Total Errors: %d/%d (%.2f%%)\n",
		totalRPS, totalErrors, totalRequests,
		float64(totalErrors)/float64(totalRequests)*100)

	// Recommandations
	fmt.Println("\n💡 RECOMMENDATIONS")
	fmt.Println("==================")

	bestRPS := 0.0
	worstRPS := 999999.0
	bestQuery := ""
	worstQuery := ""

	for _, result := range results {
		if result.RequestsPerSecond > bestRPS {
			bestRPS = result.RequestsPerSecond
			bestQuery = result.QueryName
		}
		if result.RequestsPerSecond < worstRPS {
			worstRPS = result.RequestsPerSecond
			worstQuery = result.QueryName
		}
	}

	fmt.Printf("🏆 Best performing query: %s (%.1f RPS)\n", bestQuery, bestRPS)
	fmt.Printf("🐌 Slowest query: %s (%.1f RPS)\n", worstQuery, worstRPS)

	if totalErrors > 0 {
		fmt.Printf("⚠️  Consider investigating the %d errors\n", totalErrors)
	}

	if totalRPS > 1000 {
		fmt.Println("🚀 Excellent performance! Server handles high load well")
	} else if totalRPS > 500 {
		fmt.Println("✅ Good performance for most use cases")
	} else {
		fmt.Println("⚡ Consider optimizing queries or server resources")
	}
}

// Utilitaires pour calculs statistiques
func calculateAverage(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}

func calculateMin(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	min := durations[0]
	for _, d := range durations {
		if d < min {
			min = d
		}
	}
	return min
}

func calculateMax(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	max := durations[0]
	for _, d := range durations {
		if d > max {
			max = d
		}
	}
	return max
}

func calculateAverageInt(counts []int) float64 {
	if len(counts) == 0 {
		return 0
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	return float64(total) / float64(len(counts))
}
