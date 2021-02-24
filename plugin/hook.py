import requests
from http import HTTPStatus
from time import sleep
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook


class AirbyteHook(BaseHook):

    API_URL = 'http://172.26.0.1:8001/api/v1'
    POKE_TIME_INTERVAL = 1
    WAIT_TIME_JOB_START = 5
    allowed_resource_types = [
        'destinations',
        'sources',
        'connections'
    ]
    status_running = ['running'] # ['running', 'pending']
    status_completed = ['succeeded']

        # Isso aqui deve lidar em criar uma conexão segura do Airflow
        # usando o que foi criado no UI.
        # tentar subir o Airbyte com user/pass
    def __init__(self, airbyte_conn_id: str) -> None:
        super().__init__()
        self.conn_id = airbyte_conn_id
        self.api_hook = HttpHook(http_conn_id=self.conn_id)

    def run_job(self, source_name, dest_name):

        workspace_id = self.get_workspace_id()
        source_id = self.get_source_id(workspace_id, source_name)
        dest_id = self.get_dest_id(workspace_id, dest_name)
        connection_id = self.get_connection_id(workspace_id, source_id, dest_id)

        job_response = self.trigger_sync_job(connection_id)
        job_id = job_response.get('job').get('id')
        sleep(self.WAIT_TIME_JOB_START)
        job_status = self.get_job_status(job_id)

        while job_status in self.status_running:
            sleep(self.POKE_TIME_INTERVAL)
            job_status = self.get_job_status(job_id)
        
        if job_status in self.status_completed:
            self.log.info("Job {self.job_id} is finished sync")
        else:
            raise RuntimeError("Airbyte job had some problem")

    def _get_response(self, endpoint, payload):
        self.log.info(f'Airbyte API {endpoint}')
        # r = self.api_hook.run(
        #     endpoint='/api/v1' + endpoint,
        #     data=payload,
        #     headers={'accept': 'application/json'}
        # )
        #poderia alterar para ter raise para cada error 400, 300...
        r = requests.post(
            f'{self.API_URL}{endpoint}', 
            json=payload
        )
        print(r)
        if r.status_code != HTTPStatus.OK:
            raise Exception(f"API Response: {r.status_code}")
        return r.json()

    def trigger_sync_job(self, connection_id):
        endpoint = '/connections/sync'
        payload = dict(connectionId = connection_id)
        return self._get_response(endpoint, payload)

    def get_job_status(self, job_id):
        endpoint = '/jobs/get'
        payload = dict(id = job_id)
        return self._get_response(endpoint, payload).get('job').get('status')

    def get_workspace_id(self):
        endpoint = '/workspaces/get_by_slug'
        payload = dict(slug = "string")
        return self._get_response(endpoint, payload).get('workspaceId')

    def _get_resource_info(self, resource_type, workspace_id):
        if resource_type not in self.allowed_resource_types:
            raise ValueError(f"Resource {resource_type} not recognized")
        endpoint = f'/{resource_type}/list'
        payload = dict(workspaceId = workspace_id)
        return self._get_response(endpoint, payload).get(resource_type)
    
    def _get_resource_id(self, data, resource_type, resource_name):
        for d in data:
            if d.get('name') == resource_name:
                return d.get(resource_type)
        raise ValueError(f"Resource {resource_type} with name {resource_name} not found")

    def get_source_id(self, workspace_id, source_name):
        data = self._get_resource_info('sources', workspace_id)
        return self._get_resource_id(data, 'sourceId', source_name)

    def get_dest_id(self, workspace_id, dest_name):
        data = self._get_resource_info('destinations', workspace_id)
        return self._get_resource_id(data, 'destinationId', dest_name)

    def get_connection_id(self, workspace_id, source_id, dest_id):
        data = self._get_resource_info('connections', workspace_id)
        for d in data:
            if (d.get('sourceId') == source_id) and (d.get('destinationId') == dest_id):
                return d.get('connectionId')
        raise ValueError("Airbyte connection not found.")
