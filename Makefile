.DEFAULT_GOAL := all

.PHONY: help up down build logs ps clean nuke up-grafana down-grafana logs-grafana ps-grafana clean-grafana

DOCKER_COMPOSE := $(shell command -v docker-compose || command -v docker compose)

print-docker-compose:
	@echo "Using docker-compose command: $(DOCKER_COMPOSE)"

help:
	@echo "Usage:"
	@echo "  make up               - Build Docker image and start all containers"
	@echo "  make down             - Stop all containers"
	@echo "  make build            - Build Docker image"
	@echo "  make logs             - View output from containers"
	@echo "  make ps               - List containers"
	@echo "  make clean            - Remove all stopped containers, networks, images, and volumes"
	@echo "  make nuke             - Remove all volumes, networks, containers, and images"
	@echo "  make up-grafana       - Start Grafana container"
	@echo "  make down-grafana     - Stop Grafana container"
	@echo "  make logs-grafana     - View output from Grafana container"
	@echo "  make ps-grafana       - List Grafana container"
	@echo "  make clean-grafana    - Remove Grafana stopped containers, networks, images, and volumes"

all: build up

install:
	bash install_docker.sh

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down -v

build:
	$(DOCKER_COMPOSE) build

logs:
	$(DOCKER_COMPOSE) logs -f

ps:
	$(DOCKER_COMPOSE) ps

clean:
	docker system prune -f
	docker volume prune -f

nuke:
	$(DOCKER_COMPOSE) down -v --rmi all --remove-orphans
	docker system prune -a -f
	docker volume prune -f