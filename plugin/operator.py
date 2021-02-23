import requests
from time import sleep
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class AirbyteTriggerSyncOperator(BaseOperator):
    # API_URL should be change to a airbyte_conn_id and
    # create proper http connection.
    API_URL = 'http://172.21.0.1:8001/api/v1'

    @apply_defaults
    def __init__(
            self,
            airbyte_conn_id: str,
            source_name: str,
            dest_name: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.source_name = source_name
        self.dest_name = dest_name

    def execute(self, context):
        self.workspace_id = self._get_workspace_id()
        self.source_id = self._get_source_id()
        self.destination_id = self._get_destination_id()
        self.connection_id = self._get_connection_id()
        self._trigger_sync_job()
        self._update_job_status()

        while self.job_status == 'running' or self.job_status == 'pending':
            sleep(1)
            self._update_job_status()
        
        if self.job_status == 'succeeded':
            print("Finished sync")
        else:
            raise RuntimeError("Airbyte job had some problem")

    def _get_json_response(self, path, payload):
        r = requests.post(f'{self.API_URL}{path}', json=payload)
        if r.status_code == 200:
            return r.json()

    def _trigger_sync_job(self):
        path = '/connections/sync'
        payload = dict(connectionId = self.connection_id)
        self.job_id = self._get_json_response(path, payload).get('job').get('id')

    def _update_job_status(self):
        path = '/jobs/get'
        payload = dict(id = self.job_id)
        self.job_status = self._get_json_response(path, payload).get('job').get('status')

    def _get_workspace_id(self):
        path = '/workspaces/get_by_slug'
        payload = dict(slug = "string")
        return self._get_json_response(path, payload).get('workspaceId')

    def _get_source_id(self):
        path = '/sources/list'
        payload = dict(workspaceId = self.workspace_id)
        sources = self._get_json_response(path, payload).get('sources')
        for source in sources:
            if source.get('name') == self.source_name:
                return source.get('sourceId')

    def _get_destination_id(self):
        path = '/destinations/list'
        payload = dict(workspaceId = self.workspace_id)
        destinations = self._get_json_response(path, payload).get('destinations')
        for dest in destinations:
            if dest.get('name') == self.dest_name:
                return dest.get('destinationId')

    def _get_connection_id(self):
        path = '/connections/list'
        payload = dict(workspaceId = self.workspace_id)
        connections = self._get_json_response(path, payload).get('connections')
        for conn in connections:
            if (conn.get('sourceId') == self.source_id) and (conn.get('destinationId') == self.destination_id):
                return conn.get('connectionId')
