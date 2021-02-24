from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from .hook import AirbyteHook

class AirbyteTriggerSyncOperator(BaseOperator):
    
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
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id)
        self.log.info("Calling Airbyte job")
        response = hook.run_job(self.source_name, self.dest_name)
