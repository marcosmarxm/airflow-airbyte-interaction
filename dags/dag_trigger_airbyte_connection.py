from airflow import DAG
from airflow.utils.dates import days_ago

from plugin.operator import AirbyteTriggerSyncOperator

with DAG(dag_id='trigger_airbyte_connection',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=days_ago(2)) as dag:

    money_json = AirbyteTriggerSyncOperator(
        task_id='sync_money_json',
        airbyte_conn_id='airbyte_local',
        source='Money',
        destination='JSON Destination'
    )
