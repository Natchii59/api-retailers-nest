package clickhouse

import (
	"fmt"
)

// Config contient la configuration pour ClickHouse
type Config struct {
	Host     string `envconfig:"CLICKHOUSE_HOST" default:"localhost"`
	Port     int    `envconfig:"CLICKHOUSE_PORT" default:"9000"`
	Database string `envconfig:"CLICKHOUSE_DATABASE" default:"default"`
	Username string `envconfig:"CLICKHOUSE_USERNAME" default:"default"`
	Password string `envconfig:"CLICKHOUSE_PASSWORD" default:""`
	UseTLS   bool   `envconfig:"CLICKHOUSE_USE_TLS" default:"false"`
	Debug    bool   `envconfig:"CLICKHOUSE_DEBUG" default:"false"`
}

// Validate valide la configuration
func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}

	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	if c.Database == "" {
		return fmt.Errorf("database cannot be empty")
	}

	if c.Username == "" {
		return fmt.Errorf("username cannot be empty")
	}

	return nil
}

// ConnectionString retourne la chaîne de connexion (sans mot de passe pour les logs)
func (c *Config) ConnectionString() string {
	return fmt.Sprintf("%s@%s:%d/%s", c.Username, c.Host, c.Port, c.Database)
}
