package main

import (
	"log/slog"
	"os"

	integration "webhookbroker/integration_tests"

	"github.com/jackc/pgx/v5/pgxpool"
)

type testScenario struct {
	Name string
	Run  func(pool *pgxpool.Pool) error
}

func main() {
	pool, err := integration.SetupTestDB()
	if err != nil {
		slog.Error("Failed to connect to test database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	scenarios := []testScenario{
		//{"Test failures", integration.RunScenarioRetry},
		{"Test delivering order", integration.RunScenarioOrder},
		//{"Test failure isolation", integration.RunScenarioIsolation},
	}

	failed := false

	for _, scenario := range scenarios {
		slog.Info("Test", "scenario", scenario.Name)

		if err := integration.CleanDB(pool); err != nil {
			slog.Error("Failed to clean DB before test", "error", err)
			os.Exit(1)
		}

		if err := scenario.Run(pool); err != nil {
			slog.Error("Test Failed", "scenario", scenario.Name, "error", err)

			failed = true
		} else {
			slog.Info("Test Passed", "scenario", scenario.Name)
		}
	}

	if failed {
		slog.Error("Integration test failed")

		os.Exit(1)
	}

	slog.Info("Integration test passed")
	os.Exit(0)
}
