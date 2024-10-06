SHELL := /bin/bash

all:

docker-image:
	docker build -f ./join/Dockerfile -t "join:latest" .	
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./filter_columns/Dockerfile -t "filter_columns:latest" .
	docker build -f ./drop_nulls/Dockerfile -t "drop_nulls:latest" .
	docker build -f ./counter/Dockerfile -t "counter:latest" .
	docker build -f ./top_k/Dockerfile -t "top_k:latest" .
	docker build -f ./filter_by_column_value/Dockerfile -t "filter_by_column_value:latest" .
	docker build -f ./percentile/Dockerfile -t "percentile:latest" .
.PHONY: docker-image

docker-run: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-up

docker-down:
	docker compose -f docker-compose-dev.yaml stop -t 10
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-down

docker-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs