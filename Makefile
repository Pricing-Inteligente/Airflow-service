####################################################################
# Makefile for managing Docker network
####################################################################
NETWORK=airflow_net

network:
	@if [ -z "$$(docker network ls --filter name=^${NETWORK}$$ -q)" ]; then \
		echo ">> Creando red ${NETWORK}..."; \
		docker network create ${NETWORK}; \
	else \
		echo ">> Red ${NETWORK} ya existe"; \
	fi

####################################################################
# Makefile for managing Airflow with Docker Compose
####################################################################
AIRFLOW_COMPOSE = docker compose -f docker-compose.yaml

airflow-up: network
	$(AIRFLOW_COMPOSE) up -d --build

airflow-down:
	$(AIRFLOW_COMPOSE) down

airflow-logs:
	$(AIRFLOW_COMPOSE) logs -f

airflow-ps:
	$(AIRFLOW_COMPOSE) ps

airflow-restart: airflow-down airflow-up