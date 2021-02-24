# airflow-airbyte-interaction

Draft Airflow/Airbyte integration

To clone use
```
git clone --recursive git@github.com:marcosmarxm/airflow-airbyte-interaction.git
```

Airbyte experimental [Swagger API](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-api/src/main/openapi/config.yaml&utm_content=buffer0a1ac&utm_medium=social&utm_source=linkedin.com&utm_campaign=buffer#/)

To start Airbyte run:
```bash
cd airflow-airbyte-interaction/airbyte 
docker-compose up -d
```

To start Airflow run:
```
cd airflow-airbyte-interaction
docker-compose up -d --force-recreate --remove-orphans
docker exec airflow airflow users create --username admin --password admin --role Admin --firstname A --lastname Z --email admin@email.com
```
