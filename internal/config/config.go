package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL      string
	AppPort          string
	NumWorkers       int
	PollInterval     time.Duration
	GracePeriod      time.Duration
	BaseBackoffDelay time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		AppPort:          getEnv("APP_PORT", "8080"),
		NumWorkers:       getEnvInt("NUM_WORKERS", 5),
		PollInterval:     getEnvDuration("POLL_INTERVAL", 500*time.Millisecond),
		GracePeriod:      getEnvDuration("GRACE_PERIOD", 30*time.Second),
		BaseBackoffDelay: getEnvDuration("BASE_BACKOFF_DELAY", time.Second),
	}

	if cfg.DatabaseURL == "" {
		return nil, errors.New("DATABASE_URL is required")
	}

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return defaultVal
}
