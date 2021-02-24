from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

from plugin.operator import AirbyteTriggerSyncOperator

with DAG(dag_id='trigger_airbyte_sync_job',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(2)) as dag:

    money_json = AirbyteTriggerSyncOperator(
        task_id='sync_money_json',
        airbyte_conn_id='airbyte_conn_example',
        source_name='Money',
        dest_name='JSON destination'
    )
    
    another_task = DummyOperator(
        task_id='downstream_pipeline'
    )

    money_json >> another_task
