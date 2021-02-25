from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

from plugin.operator import AirbyteTriggerSyncOperator


with DAG(dag_id='airbyte_sync_job',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(2)) as dag:

    source_destination = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_source_dest',
        airbyte_conn_id='airbyte_conn_example',
        connection_id='15bc3800-82e4-48c3-a32d-620661273f28'
    )

    split_train_test = DummyOperator(task_id='split_train_test')
    
    create_cluster = DummyOperator(task_id='create_cluster')

    ml_model_update = DummyOperator(task_id='update_ml_model')

    source_destination >> split_train_test >> ml_model_update
    create_cluster >> ml_model_update
