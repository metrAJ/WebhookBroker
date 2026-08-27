package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"
)

func main() {
	apiURL := os.Getenv("API_URL")
	if apiURL == "" {
		slog.Error("API_URL is missing")
		os.Exit(1)
	}

	time.Sleep(2 * time.Second)

	start := time.Now()

	var hookSleepy, hookCorrectOrder, hookUnresponsive *MockHook

	hookCorrectOrder = NewMockHook("Order", nil)
	defer hookCorrectOrder.Close()

	hookUnresponsive = NewMockHook("Fail", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer hookUnresponsive.Close()

	hookSleepy = NewMockHook("Sleepy", func(w http.ResponseWriter, r *http.Request) {
		if time.Since(start) < 10*time.Second {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		bodyBytes, _ := io.ReadAll(r.Body)
		cleanData := strings.Trim(strings.TrimSpace(string(bodyBytes)), `"`)

		slog.Info("Sleepy Hook received request",
			slog.String("raw_body", string(bodyBytes)),
			slog.String("cleaned_data", cleanData),
		)

		hookSleepy.Events <- cleanData

		w.WriteHeader(http.StatusOK)
	})
	defer hookSleepy.Close()

	registerWebhook(apiURL, hookCorrectOrder.URL, `{"dataStartsWith": "O"}`)
	registerWebhook(apiURL, hookUnresponsive.URL, `{}`)
	registerWebhook(apiURL, hookSleepy.URL, `{"dataStartsWith": "S"}`)

	fireEvent(apiURL, "Super_Late")
	fireEvent(apiURL, "Ok_1")
	fireEvent(apiURL, "Bad_1")
	fireEvent(apiURL, "Ok_2")
	fireEvent(apiURL, "Bad_2")
	fireEvent(apiURL, "Ok_3")

	var (
		okAmount   = 3
		lateAmount = 1
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := hookCorrectOrder.Validate(ctx, okAmount, func(got []string) error {
		slog.Info("Order Hook Validation", slog.Any("received_array", got))

		if got[0] != "Ok_1" || got[1] != "Ok_2" || got[2] != "Ok_3" {
			return fmt.Errorf("ordering failed: %v", got)
		}

		return nil
	}); err != nil {
		slog.Error("Test failed", "error", err)
		os.Exit(1)
	}

	if err := hookSleepy.Validate(ctx, lateAmount, nil); err != nil {
		slog.Error("Test failed", "error", err)
		os.Exit(1)
	}

	slog.Info("E2E tests passed.")
	os.Exit(0)
}

type MockHook struct {
	Name   string
	Server *httptest.Server
	URL    string
	Events chan string
}

func NewMockHook(name string, customHandler http.HandlerFunc) *MockHook {
	hook := &MockHook{
		Name:   name,
		Events: make(chan string, 50),
	}
	handler := customHandler

	if handler == nil {
		handler = func(w http.ResponseWriter, r *http.Request) {
			bodyBytes, _ := io.ReadAll(r.Body)
			cleanData := strings.Trim(strings.TrimSpace(string(bodyBytes)), `"`)

			slog.Info("Mock Server received request",
				slog.String("hook_name", hook.Name),
				slog.String("raw_body", string(bodyBytes)),
				slog.String("cleaned_data", cleanData),
			)

			hook.Events <- cleanData

			w.WriteHeader(http.StatusOK)
		}
	}

	hook.Server = httptest.NewUnstartedServer(handler)

	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		slog.Error("Failed to start mock server")
		os.Exit(1)
	}

	hook.Server.Listener = l
	hook.Server.Start()

	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		slog.Error("Failed to get free port")
		os.Exit(1)
	}

	hook.URL = fmt.Sprintf("http://e2e-tester:%s", port)

	return hook
}

func (mh *MockHook) Close() {
	mh.Server.Close()
	close(mh.Events)
}

func (mh *MockHook) Validate(ctx context.Context, expectedEvents int, verifyFn func([]string) error) error {
	var received []string
	for len(received) < expectedEvents {
		select {
		case <-ctx.Done():
			return fmt.Errorf("[%s] timeout: got %d/%d events", mh.Name, len(received), expectedEvents)
		case ev := <-mh.Events:
			received = append(received, ev)
		}
	}

	if verifyFn != nil {
		return verifyFn(received)
	}

	return nil
}

func registerWebhook(apiURL, hookURL, filters string) {
	body := fmt.Sprintf(`{"hook_url": "%s", "filters": %s}`, hookURL, filters)
	if filters == "{}" {
		body = fmt.Sprintf(`{"hook_url": "%s"}`, hookURL)
	}

	resp, err := http.Post(apiURL+"/webhooks", "application/json", bytes.NewBuffer([]byte(body)))
	if err != nil || resp.StatusCode != http.StatusCreated {
		slog.Error("Failed to register webhook", "url", hookURL)
		os.Exit(1)
	}
}

func fireEvent(apiURL, data string) {
	body := fmt.Sprintf(`{"id": "550e8400-e29b-41d4-a716-446655440000", "issuer": "Aboba", "data": "%s"}`, data)

	resp, err := http.Post(apiURL+"/events", "application/json", bytes.NewBuffer([]byte(body)))
	if err != nil || resp.StatusCode != http.StatusAccepted {
		slog.Error("Failed to fire event", "data", data)
		os.Exit(1)
	}
}
