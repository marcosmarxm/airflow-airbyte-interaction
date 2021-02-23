# airflow-airbyte-interaction

To clone use
```
git clone --recursive 
```

Airbyte experimental API: https://petstore.swagger.io/?url=https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-api/src/main/openapi/config.yaml&utm_content=buffer0a1ac&utm_medium=social&utm_source=linkedin.com&utm_campaign=buffer#/

Draft Airflow/Airbyte integration

```bash
cd airbyte 
docker-compose up -d
cd ..
docker-compose up -d
docker exec airflow airflow users create --username admin --password admin --role Admin --firstname A --lastname Z --email admin@email.com
```
