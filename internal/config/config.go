package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DB         DBConfig
	API        APIConfig
	Dispatcher DispatcherConfig
}

type DBConfig struct {
	DSN      string
	MaxConns int
	MinConns int
}

type APIConfig struct {
	Port string
}

type DispatcherConfig struct {
	MaxRetries   int
	BaseSecWait  time.Duration
	WorkerCount  int
	TaskLeaseSec int
}

func getEnv(key, def string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return def
}

func getEnvAsInt(key string, def int) int {
	strValue := getEnv(key, "")
	if strValue == "" {
		return def
	}

	value, err := strconv.Atoi(strValue)
	if err != nil {
		return def
	}

	return value
}

func Load() Config {
	return Config{
		DB: DBConfig{
			DSN:      getEnv("DB_URL", "postgres://user:password@localhost:5433/broker?sslmode=disable"),
			MaxConns: getEnvAsInt("DB_MAX_CONNS", 25),
			MinConns: getEnvAsInt("DB_MIN_CONNS", 5),
		},
		API: APIConfig{
			Port: getEnv("API_PORT", "8080"),
		},
		Dispatcher: DispatcherConfig{
			MaxRetries:   getEnvAsInt("DISPATCH_MAX_RETRIES", 9),
			BaseSecWait:  time.Duration(getEnvAsInt("DISPATCH_BASE_WAIT_SEC", 10)) * time.Second,
			WorkerCount:  getEnvAsInt("DISPATCH_WORKER_COUNT", 10),
			TaskLeaseSec: getEnvAsInt("DISPATCH_LEASE_SEC", 30),
		},
	}
}
