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

        self._airbyte_api = 'http://airbyte:8000'
        self._connection_id = '15bc3800-82e4-48c3-a32d-620661273f28'

    def execute(self, context):
        job = self._sync_connection()
        job_id = job.get('header').get('')
        if job.status_code == 200:
            print("YEAH!")


    def _sync_connection(self):
        payload = {"connectionId": self._connection_id}
        r = request.post(f'{self.airbyte_api}/v1/connections/sync', json=payload)
        return r