.PHONY: help start stop restart status logs health clean

CYAN  := \033[0;36m
GREEN := \033[0;32m
RESET := \033[0m

help:
	@echo ""
	@echo "$(CYAN)ILAM — Data Lakehouse Platform$(RESET)"
	@echo "  make start     Start all services"
	@echo "  make stop      Stop all services"
	@echo "  make status    Show services status"
	@echo "  make health    Check services health"
	@echo "  make logs      Tail all logs"
	@echo "  make clean     Remove containers and volumes"
	@echo ""
	@echo "$(GREEN)URLs$(RESET)"
	@echo "  MinIO    → http://localhost:9001"
	@echo "  Trino    → http://localhost:8080"
	@echo "  Flink    → http://localhost:8081"
	@echo "  Airflow  → http://localhost:8082"
	@echo "  Grafana  → http://localhost:3000"
	@echo ""

start:
	@echo "$(CYAN)Starting ILAM...$(RESET)"
	@mkdir -p orchestration/dags orchestration/logs transform
	docker compose up -d
	@echo "$(GREEN)Done. Run 'make status' to check.$(RESET)"

stop:
	docker compose down

restart: stop start

status:
	docker compose ps

logs:
	docker compose logs -f --tail=100

health:
	@echo "$(CYAN)Checking services health...$(RESET)"
	@echo -n "MinIO:      "; curl -sf http://localhost:9000/minio/health/live && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Iceberg REST: "; curl -sf http://localhost:8181/v1/config > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Trino:      "; curl -sf http://localhost:8080/v1/info > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Flink:      "; curl -sf http://localhost:8081/overview > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Airflow:    "; curl -sf http://localhost:8082/health > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Prometheus: "; curl -sf http://localhost:9090/-/healthy > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Grafana:    "; curl -sf http://localhost:3000/api/health > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"

clean:
	docker compose down -v --remove-orphans

trino-cli:
	docker exec -it ilam-trino trino --catalog iceberg

init-warehouse:
	@echo "$(CYAN)Initializing Iceberg warehouse...$(RESET)"
	docker exec -i ilam-trino trino --catalog iceberg < warehouse/bronze/ddl/create_bronze.sql
	docker exec -i ilam-trino trino --catalog iceberg < warehouse/silver/ddl/create_silver.sql
	docker exec -i ilam-trino trino --catalog iceberg < warehouse/gold/ddl/create_gold.sql
	@echo "$(GREEN)Warehouse initialized.$(RESET)"

show-tables:
	@echo "$(CYAN)Listing all Iceberg tables...$(RESET)"
	docker exec -i ilam-trino trino --catalog iceberg --execute \
		"SELECT table_schema, table_name FROM iceberg.information_schema.tables WHERE table_schema IN ('bronze','silver','gold') ORDER BY 1,2;"
