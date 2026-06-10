# ==============================
# Docker Compose Service Manager
# ==============================

# Docker Compose command for local testing on this machine.
COMPOSE_CMD ?= docker-compose

# Danh sách service
SERVICES := kafka spark minio postgre redis airflow trino agent frontend monitoring
STACK_SERVICES := kafka minio spark airflow trino redis agent frontend monitoring
BUILD_SERVICES := airflow agent frontend

# Tên file compose
COMPOSE_kafka    := docker-compose.kafka.yml
COMPOSE_spark    := docker-compose.spark.yml
COMPOSE_minio    := docker-compose.minio.yml
COMPOSE_postgre := docker-compose.postgre.yml
COMPOSE_redis    := docker-compose.redis.yml
COMPOSE_airflow := docker-compose.airflow.yml
COMPOSE_trino    := docker-compose.trino.yml
COMPOSE_agent    := docker-compose.agent.yml
COMPOSE_frontend := docker-compose.frontend.yml
COMPOSE_monitoring := docker-compose.monitoring.yml

STACK_COMPOSE_FILES := $(foreach svc,$(STACK_SERVICES),-f $(COMPOSE_$(svc)))

# ==============================
# Helper macro
# ==============================

define compose_up
	$(COMPOSE_CMD) -f $(1) up -d --no-build
endef

define compose_build
	$(COMPOSE_CMD) -f $(1) build
endef

define compose_down
	$(COMPOSE_CMD) -f $(1) down
endef

define compose_logs
	$(COMPOSE_CMD) -f $(1) logs -f
endef

define compose_restart
	$(COMPOSE_CMD) -f $(1) restart
endef

# ==============================
# Generate commands automatically
# ==============================

.PHONY: network all-up all-down all-restart all-build ps help ui-up ui-down ui-logs ui-restart ui-build \
	$(addsuffix -up,$(SERVICES)) \
	$(addsuffix -build,$(BUILD_SERVICES)) \
	$(addsuffix -down,$(SERVICES)) \
	$(addsuffix -logs,$(SERVICES)) \
	$(addsuffix -restart,$(SERVICES))

network:
	@docker network inspect data_network >/dev/null 2>&1 || docker network create data_network

$(foreach svc,$(SERVICES),\
$(eval $(svc)-up: network ; @$(call compose_up,$(COMPOSE_$(svc)))) \
$(eval $(svc)-down: ; @$(call compose_down,$(COMPOSE_$(svc)))) \
$(eval $(svc)-logs: ; @$(call compose_logs,$(COMPOSE_$(svc)))) \
$(eval $(svc)-restart: ; @$(call compose_restart,$(COMPOSE_$(svc)))) \
)

$(foreach svc,$(BUILD_SERVICES),\
$(eval $(svc)-build: ; @$(call compose_build,$(COMPOSE_$(svc)))) \
)

ui-up: frontend-up
ui-build: frontend-build
ui-down: frontend-down
ui-logs: frontend-logs
ui-restart: frontend-restart

# ==============================
# Start all services
# ==============================

all-up: network
	$(COMPOSE_CMD) $(STACK_COMPOSE_FILES) up -d --no-build

# ==============================
# Build images that need local Dockerfiles
# ==============================

all-build:
	$(COMPOSE_CMD) $(foreach svc,$(BUILD_SERVICES),-f $(COMPOSE_$(svc))) build

# ==============================
# Stop all services
# ==============================

all-down:
	$(COMPOSE_CMD) $(STACK_COMPOSE_FILES) down --remove-orphans

# ==============================
# Restart all
# ==============================

all-restart:
	$(COMPOSE_CMD) $(STACK_COMPOSE_FILES) restart

# ==============================
# Show status
# ==============================

ps:
	$(COMPOSE_CMD) $(STACK_COMPOSE_FILES) ps

# ==============================
# Help
# ==============================

help:
	@echo ""
	@echo "Available commands:"
	@echo "  make kafka-up"
	@echo "  make kafka-down"
	@echo "  make kafka-logs"
	@echo "  make kafka-restart"
	@echo ""
	@echo "  make spark-up"
	@echo "  make minio-up"
	@echo "  make postgre-up"
	@echo "  make redis-up"
	@echo "  make airflow-build"
	@echo "  make airflow-up"
	@echo "  make agent-build"
	@echo "  make agent-up"
	@echo "  make frontend-build"
	@echo "  make frontend-up"
	@echo "  make trino-up"
	@echo "  make ui-build"
	@echo "  make ui-up"
	@echo ""
	@echo "  make all-build"
	@echo "  make all-up"
	@echo "  make all-down"
	@echo "  make all-restart"
	@echo "  make ps"
