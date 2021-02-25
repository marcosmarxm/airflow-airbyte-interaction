import requests
from http import HTTPStatus
from time import sleep
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook

class AirbyteJobController:
    """Airbyte job status"""
    RUNNING = 'running'
    COMPLETED = 'completed'
    PENDING = 'pending'
    FAILED = 'failed'


class AirbyteHook(BaseHook):
    """Hook for Airbyte API"""

    def __init__(self, airbyte_conn_id: str) -> None:
        super().__init__()
        self.conn_id = airbyte_conn_id
        self.api_hook = HttpHook(http_conn_id=self.conn_id)

    def submit_job(self, connection_id) -> dict:
        """
        Submits a job to a Airbyte server.
        :param connection_id: Required. The ConnectionId of the Airbyte Connection.
        :type connectiond_id: str
        """
        return self.api_hook.run(
            endpoint='/api/v1/connections/sync,
            data={'connectionId': connnection_id},
            headers={'accept': 'application/json'}
        )

    def wait_for_job(
        self, job_id: str, wait_time: int = 3, timeout: Optional[int] = None
    ) -> None:
        """
        Helper method which polls a job to check if it finishes.
        :param job_id: Id of the Airbyte job
        :type job_id: str
        :param wait_time: Number of seconds between checks
        :type wait_time: int
        :param timeout: How many seconds wait for job to be ready. Used only if ``asynchronous`` is False
        :type timeout: int
        """
        state = None
        start = time.monotonic()
        while state not in (AirbyteJobStatus.ERROR, AirbyteJobStatus.DONE, AirbyteJobStatus.CANCELLED):
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(f"Timeout: dataproc job {job_id} is not ready after {timeout}s")
            time.sleep(wait_time)
            try:
                job = self.get_job(job_id=job_id)
                state = job.get('job').get('status')
            except ServerError as err:
                self.log.info("Retrying. Airbyte API returned server error when waiting for job: %s", err)

        if state == AirbyteJobStatus.ERROR:
            raise AirflowException(f'Job failed:\n{job}')
        if state == AirbyteJobStatus.CANCELLED:
            raise AirflowException(f'Job was cancelled:\n{job}')

    def get_job(self, job_id: str) -> dict:
        """
        Gets the resource representation for a job in Airbyte.
        :param job_id: Id of the Airbyte job
        :type job_id: str
        """
        return self.api_hook.run(
            endpoint='/api/v1/job/get,
            data={'id': job_id},
            headers={'accept': 'application/json'}
        )
