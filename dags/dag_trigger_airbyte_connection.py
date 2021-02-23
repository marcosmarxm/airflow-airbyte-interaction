import requests
from time import sleep
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class AirbyteTriggerSyncOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            airbyte_conn_id: str,
            source: str,
            destination: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.source = source
        self.destination = destination
        self._airbyte_api = 'http://172.21.0.1:8001/api'
        self._connection_id = '15bc3800-82e4-48c3-a32d-620661273f28'

    def execute(self, context):
        self._sync_connection()
        while self.job_status == 'running' or self.job_status == 'pending':
            sleep(1)
            self.job_status = self._get_job_status()
        
        if self.job_status == 'succeeded':
            print("Finished sync")
        else:
            raise("Something weird happened!")

    def _sync_connection(self):
        payload = {"connectionId": self._connection_id}
        r = requests.post(f'{self._airbyte_api}/v1/connections/sync', json=payload)
        if r.status_code == 200:
            self.job_id = r.get('job').get('id')
        else:
            raise Error("Something wrong with Airbyte job")

    def _get_job_status(self):
        payload = {"id": self.job_id}
        r = requests.post(f'{self._airbyte_api}/v1/jobs/get', json=payload)
        if r.status_code == 200:
            self.job_status = r.get('job').get('status')
        else:
            raise Error("Something wrong with Airbyte job")


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

# status = "running", "pending", "su"