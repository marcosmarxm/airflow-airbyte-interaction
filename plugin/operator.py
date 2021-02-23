import requests
from time import sleep
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
        self._airbyte_api = 'http://172.21.0.1:8001/api/v1'
        self._connection_id = '15bc3800-82e4-48c3-a32d-620661273f28'

    def execute(self, context):
        self.job_id = self._sync_connection()
        self.job_status = self._get_job_status()

        while self.job_status == 'running' or self.job_status == 'pending':
            sleep(1)
            self.job_status = self._get_job_status()
        
        if self.job_status == 'succeeded':
            print("Finished sync")
        else:
            raise("Something weird happened!")

    def _get_json_response(self, path, payload):
        r = requests.post(f'{self._airbyte_api}{path}', json=payload)
        if r.status_code == 200:
            return r.json()

    def _sync_connection(self):
        path = '/connections/sync'
        payload = {"connectionId": self._connection_id}
        return _get_json_response(path, payload).get('job').get('id')

    def _get_job_status(self):
        path = '/jobs/get'
        payload = {"id": self.job_id}
        return _get_json_response(path, payload).get('job').get('status')