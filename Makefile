airflow:
	docker-compose up -d --force-recreate --remove-orphans
	
airbyte:
	docker-compose up -d -f airbyte/docker-compose.yaml
