package clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client gère la connexion à ClickHouse
type Client struct {
	conn   driver.Conn
	db     *sql.DB
	config *Config
}

// NewClient crée un nouveau client ClickHouse
func NewClient(config *Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Configuration des options de connexion
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Debug: config.Debug,
		Debugf: func(format string, v ...interface{}) {
			if config.Debug {
				log.Printf("[ClickHouse Debug] "+format, v...)
			}
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "clickhouse-connector", Version: "0.1"},
			},
		},
	}

	// Configuration TLS conditionnelle
	if config.UseTLS {
		options.TLS = &tls.Config{
			InsecureSkipVerify: true, // Pour développement - à changer en production
		}
	}

	// Connexion SQL pour les requêtes complexes
	db := clickhouse.OpenDB(options)

	// Test de la connexion SQL
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse database: %w", err)
	}

	// Connexion native pour les requêtes simples
	conn, err := clickhouse.Open(options)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to open ClickHouse native connection: %w", err)
	}

	// Test de la connexion native
	if err := conn.Ping(ctx); err != nil {
		db.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse native connection: %w", err)
	}

	return &Client{
		conn:   conn,
		db:     db,
		config: config,
	}, nil
}

// ValidateQuery valide que la requête SQL est sécurisée
func (c *Client) ValidateQuery(query string) error {
	queryLower := strings.ToLower(strings.TrimSpace(query))

	// Vérifier que c'est bien une requête SELECT
	if !strings.HasPrefix(queryLower, "select") {
		return fmt.Errorf("only SELECT queries are allowed")
	}

	// Blacklist des mots clés dangereux (plus précise)
	dangerousKeywords := []string{
		" drop ", " delete ", " insert ", " update ", " alter ",
		" truncate ", " grant ", " revoke ", " exec ", " execute ",
		"drop table", "drop database", "create table", "create database",
		"alter table", "truncate table", "insert into",
	}

	// Ajouter des espaces pour éviter les faux positifs
	queryWithSpaces := " " + queryLower + " "

	for _, keyword := range dangerousKeywords {
		if strings.Contains(queryWithSpaces, keyword) {
			return fmt.Errorf("query contains forbidden keyword: %s", strings.TrimSpace(keyword))
		}
	}

	return nil
}

// DetectColumns détecte quelles colonnes sont présentes dans la requête SELECT
func (c *Client) DetectColumns(query string) (map[string]bool, error) {
	// Cette fonction fait une analyse simple de la requête pour détecter les colonnes
	queryLower := strings.ToLower(query)

	// Extraire la partie SELECT
	selectIndex := strings.Index(queryLower, "select")
	fromIndex := strings.Index(queryLower, "from")

	if selectIndex == -1 || fromIndex == -1 || fromIndex <= selectIndex {
		return nil, fmt.Errorf("invalid SELECT query format")
	}

	selectPart := query[selectIndex+6 : fromIndex] // +6 pour "select"
	selectPart = strings.TrimSpace(selectPart)

	columns := map[string]bool{
		"id":         false,
		"name":       false,
		"created_at": false,
	}

	// Si c'est SELECT *, toutes les colonnes sont présentes
	if strings.Contains(selectPart, "*") {
		for col := range columns {
			columns[col] = true
		}
		return columns, nil
	}

	// Analyser les colonnes individuelles
	selectLower := strings.ToLower(selectPart)
	if strings.Contains(selectLower, "id") {
		columns["id"] = true
	}
	if strings.Contains(selectLower, "name") {
		columns["name"] = true
	}
	if strings.Contains(selectLower, "created_at") {
		columns["created_at"] = true
	}

	return columns, nil
}

// SQLResult représente le résultat brut d'une requête SQL
type SQLResult struct {
	Columns []string
	Rows    []map[string]interface{}
}

// ExecuteQuery exécute une requête SQL et retourne les résultats bruts
func (c *Client) ExecuteQuery(ctx context.Context, sqlQuery string, parameters map[string]string, limit, offset int32) (*SQLResult, int64, error) {
	start := time.Now()

	// Validation de la requête
	if sqlQuery == "" {
		return nil, 0, fmt.Errorf("sql_query cannot be empty")
	}

	if err := c.ValidateQuery(sqlQuery); err != nil {
		return nil, 0, fmt.Errorf("invalid query: %w", err)
	}

	log.Printf("Executing SQL query: %s", sqlQuery)

	// Préparer les paramètres
	var args []interface{}
	finalQuery := sqlQuery

	// Remplacer les paramètres nommés par des ? dans l'ordre
	for i := 1; i <= len(parameters); i++ {
		paramKey := fmt.Sprintf("%d", i)
		if paramValue, exists := parameters[paramKey]; exists {
			args = append(args, paramValue)
		}
	}

	// Ajouter la pagination si spécifiée
	if limit > 0 {
		finalQuery += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		finalQuery += fmt.Sprintf(" OFFSET %d", offset)
	}

	// Exécuter la requête
	rows, err := c.db.QueryContext(ctx, finalQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Analyser les colonnes
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get column names: %w", err)
	}

	// Préparer le scan
	columnValues := make([]interface{}, len(columnNames))
	columnPointers := make([]interface{}, len(columnNames))
	for i := range columnValues {
		columnPointers[i] = &columnValues[i]
	}

	// Scanner toutes les rows
	var resultRows []map[string]interface{}

	for rows.Next() {
		err := rows.Scan(columnPointers...)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan row: %w", err)
		}

		// Créer une map pour cette row
		rowMap := make(map[string]interface{})
		for i, columnName := range columnNames {
			rowMap[columnName] = columnValues[i]
		}

		resultRows = append(resultRows, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error iterating rows: %w", err)
	}

	executionTime := time.Since(start).Milliseconds()

	result := &SQLResult{
		Columns: columnNames,
		Rows:    resultRows,
	}

	log.Printf("Query executed successfully: %d rows returned in %dms", len(resultRows), executionTime)
	return result, executionTime, nil
}

// TestConnection teste la connexion ClickHouse (pour health check)
func (c *Client) TestConnection(ctx context.Context) error {
	testCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.db.QueryContext(testCtx, "SELECT 1")
	return err
}

// Close ferme les connexions
func (c *Client) Close() error {
	var dbErr, connErr error

	if c.db != nil {
		dbErr = c.db.Close()
	}

	if c.conn != nil {
		connErr = c.conn.Close()
	}

	if dbErr != nil {
		return fmt.Errorf("failed to close database connection: %w", dbErr)
	}

	if connErr != nil {
		return fmt.Errorf("failed to close native connection: %w", connErr)
	}

	return nil
}
