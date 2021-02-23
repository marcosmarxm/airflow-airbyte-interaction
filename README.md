# airflow-airbyte-interaction

Draft Airflow/Airbyte integration

```bash
cd airbyte 
docker-compose up -d
cd ..
docker-compose up -d
docker exec airflow airflow users create --username admin --password admin --role Admin --firstname A --lastname Z --email admin@email.com
```
