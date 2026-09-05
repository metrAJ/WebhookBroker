.PHONY: help init server broker test rm-db rm-test default golint

default: help

help:
	@echo "usage: make [target]"
	@echo "targets:"
	@echo "	init    		Start DB Container"
	@echo "	server			Start server"
	@echo "	broker			Start broker"
	@echo "	filter			Start filter"
	@echo "	test			Start Tests on separate container and DB. Results will be shown in console, can take 10-15 sec to start."
	@echo "	rm-db			Remove DB container and mounts"
	@echo "	rm-test			Remove test and test-db containers"
	@echo "	golint			Run golangci-lint check"
	@echo "	precomm			Run precommit check"
	@echo "	e2e				Run e2e test with real service and all components on separate DB. Can take some time to build all services"
	@echo "	rm-e2e			Remove e2e containers and mounts"

server:
	go run ./cmd/server/

broker:
	go run ./cmd/dispatcher/

filer:
	go run ./cmd/filter/

init:
	docker-compose up -d

test: 
	docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from integration-tests

rm-db:
	docker-compose down -v

rm-test:
	docker compose -f docker-compose.test.yml down -v

golint: 
	golangci-lint run

precomm:
	pre-commit run --all-files

e2e:
	BUILDKIT_PARALLEL_LIMIT=4 docker compose -f docker-compose.e2e.yml up --build --abort-on-container-exit

rm-e2e:
	docker compose -f docker-compose.e2e.yml down -v
