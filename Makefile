SHELL := /bin/bash

all:

docker-image:
	docker build -f ./join/Dockerfile -t "join:latest" .	
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./filter_columns/Dockerfile -t "filter_columns:latest" .
	docker build -f ./drop_nulls/Dockerfile -t "drop_nulls:latest" .
	docker build -f ./top_k/Dockerfile -t "top_k:latest" .
	docker build -f ./filter_by_column_value/Dockerfile -t "filter_by_column_value:latest" .
	docker build -f ./counter_by_platform/Dockerfile -t "counter_by_platform:latest" .
	docker build -f ./counter_by_app_id/Dockerfile -t "counter_by_app_id:latest" .
	docker build -f ./percentile/Dockerfile -t "percentile:latest" .
.PHONY: docker-image

docker-run: docker-image
	python3 generate_compose.py docker-compose-dev.yaml
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-up

docker-down:
	docker compose -f docker-compose-dev.yaml stop -t 10
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-down

docker-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

docker-restart:
	docker compose -f docker-compose-dev.yaml stop -t 10
	docker compose -f docker-compose-dev.yaml down
	python3 generate_compose.py docker-compose-dev.yaml
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-restart
