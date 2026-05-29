# ==============================
# Docker Compose Service Manager
# ==============================

# Danh sách service
SERVICES := kafka spark minio postgre airflow trino app

# Tên file compose
COMPOSE_kafka    := docker-compose.kafka.yml
COMPOSE_spark    := docker-compose.spark.yml
COMPOSE_minio    := docker-compose.minio.yml
COMPOSE_postgre := docker-compose.postgre.yml
COMPOSE_airflow := docker-compose.airflow.yml
COMPOSE_trino    := docker-compose.trino.yml
COMPOSE_app      := docker-compose.app.yml

# ==============================
# Helper macro
# ==============================

define compose_up
	docker compose -f $(1) up -d
endef

define compose_down
	docker compose -f $(1) down
endef

define compose_logs
	docker compose -f $(1) logs -f
endef

define compose_restart
	docker compose -f $(1) restart
endef

# Build images from source (needed after editing app/backend or app/frontend).
define compose_build
	docker compose -f $(1) build
endef

# Rebuild from source AND recreate containers — use this to pick up code changes
# (e.g. the frontend theme toggle). Plain `*-up` reuses the cached image.
define compose_rebuild
	docker compose -f $(1) up -d --build --force-recreate
endef

# ==============================
# Generate commands automatically
# ==============================

$(foreach svc,$(SERVICES),\
$(eval $(svc)-up: ; @$(call compose_up,$(COMPOSE_$(svc)))) \
$(eval $(svc)-down: ; @$(call compose_down,$(COMPOSE_$(svc)))) \
$(eval $(svc)-logs: ; @$(call compose_logs,$(COMPOSE_$(svc)))) \
$(eval $(svc)-restart: ; @$(call compose_restart,$(COMPOSE_$(svc)))) \
$(eval $(svc)-build: ; @$(call compose_build,$(COMPOSE_$(svc)))) \
$(eval $(svc)-rebuild: ; @$(call compose_rebuild,$(COMPOSE_$(svc)))) \
)

# ==============================
# Start all services
# ==============================

all-up:
	@for svc in $(SERVICES); do \
		echo "Starting $$svc..."; \
		docker compose -f docker-compose.$$svc.yml up -d; \
	done

# ==============================
# Stop all services
# ==============================

all-down:
	@for svc in $(SERVICES); do \
		echo "Stopping $$svc..."; \
		docker compose -f docker-compose.$$svc.yml down; \
	done

# ==============================
# Restart all
# ==============================

all-restart:
	@for svc in $(SERVICES); do \
		echo "Restarting $$svc..."; \
		docker compose -f docker-compose.$$svc.yml restart; \
	done

# ==============================
# Show status
# ==============================

ps:
	@for svc in $(SERVICES); do \
		echo "===== $$svc ====="; \
		docker compose -f docker-compose.$$svc.yml ps; \
	done

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
	@echo "  make airflow-up"
	@echo "  make trino-up"
	@echo "  make app-up           # Analytics Console (cached image)"
	@echo "  make app-rebuild      # Rebuild from source + recreate (pick up code changes)"
	@echo "  make app-build        # Build images only"
	@echo ""
	@echo "  make all-up"
	@echo "  make all-down"
	@echo "  make all-restart"
	@echo "  make ps"