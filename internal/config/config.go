package config

import (
	"fmt"
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
	Port uint16
}

type DispatcherConfig struct {
	MaxRetries   int
	BaseSecWait  time.Duration
	WorkerCount  int
	TaskLeaseSec int
}

func Load() (Config, error) {
	maxConns, err := getEnvAsInt("DB_MAX_CONNS", 25)
	if err != nil {
		return Config{}, err
	}

	minConns, err := getEnvAsInt("DB_MIN_CONNS", 5)
	if err != nil {
		return Config{}, err
	}

	apiPort, err := getEnvAsUint16("API_PORT", 8080)
	if err != nil {
		return Config{}, err
	}

	maxRetries, err := getEnvAsInt("DISPATCH_MAX_RETRIES", 9)
	if err != nil {
		return Config{}, err
	}

	baseWaitSec, err := getEnvAsInt("DISPATCH_BASE_WAIT_SEC", 10)
	if err != nil {
		return Config{}, err
	}

	workerCount, err := getEnvAsInt("DISPATCH_WORKER_COUNT", 10)
	if err != nil {
		return Config{}, err
	}

	leaseSec, err := getEnvAsInt("DISPATCH_LEASE_SEC", 30)
	if err != nil {
		return Config{}, err
	}

	return Config{
		DB: DBConfig{
			DSN:      getEnv("DB_URL", "postgres://user:password@localhost:5433/broker?sslmode=disable"),
			MaxConns: maxConns,
			MinConns: minConns,
		},
		API: APIConfig{
			Port: apiPort,
		},
		Dispatcher: DispatcherConfig{
			MaxRetries:   maxRetries,
			BaseSecWait:  time.Duration(baseWaitSec) * time.Second,
			WorkerCount:  workerCount,
			TaskLeaseSec: leaseSec,
		},
	}, nil
}

func getEnv(key, def string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return def
}

func getEnvAsInt(key string, def int) (int, error) {
	strValue := getEnv(key, "")
	if strValue == "" {
		return def, nil
	}

	value, err := strconv.Atoi(strValue)
	if err != nil {
		return 0, fmt.Errorf("config/getEnvAsInt invalid integer value for %s: %w", key, err)
	}

	return value, nil
}

func getEnvAsUint16(key string, def uint16) (uint16, error) {
	strValue := getEnv(key, "")
	if strValue == "" {
		return def, nil
	}

	value, err := strconv.ParseUint(strValue, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("config/getEnvAsUint16 invalid port value for %s: %w", key, err)
	}

	return uint16(value), nil
}
