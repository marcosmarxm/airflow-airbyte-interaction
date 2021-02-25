import threading
import time
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.http.hooks.http import HttpHook

class AirbyteTriggerSyncOperator(BaseOperator):
    """
    Submit a job to an Airbyte server
    
    :param airbyte_conn_id: Required. The Airflow connection to communicate
    :type airbyte_conn_id: str
    :param connection_id: Required. The Airbyte ConnectionId between Source and Destination
    :type connection_id: str
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete. 
    :type timeout: float
    """

    @apply_defaults
    def __init__(
            self,
            airbyte_conn_id: str,
            connection_id: str,
            timeout: int = 3600,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.connection_id = connection_id
        self.timeout = timeout
        
    def execute(self, context):
        """Create Airbyte Job and wait to finish"""
        hook = HttpHook(airbyte_conn_id=self.airbyte_conn_id)

        job_object = hook.submit_job(connection_id=self.connection_id)
        job_id = job_object.get('job').get('id')

        hook.wait_for_job(job_id=job_id, self.timeout)
        self.log.info('Job %s completed successfully', job_id)
        self.job_id = job_id
        return self.job_id


    def _monitor_job_status(self, job_id):
        while self._get_job_status(job_id).get('job').get('status') in self.valid_states:
            time.sleep(1)

    def _get_job_status(self):
        return hook.run(
            endpoint='/api/v1/jobs/get',
            data={'id': self.job_id}
        )
