
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import unittest
import time
from time import sleep
from unittest import mock
import requests_mock

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.version import version
#from airflow.providers.airbyte.hooks import AirbyteHook

from plugin.operator import AirbyteTriggerSyncOperator
from plugin.hook import AirbyteHook, AirbyteJobController

AIRFLOW_VERSION = "v" + version.replace(".", "-").replace("+", "-")
AIRBYTE_STRING = "airflow.providers.airbyte.operators.{}"

AIRBYTE_CONN_ID = 'test_airbyte_conn_id'
AIRBYTE_CONNECTION = 'test_airbyte_connection'
JOB_ID = 1
TIMEOUT = 3600


def get_airbyte_connection(unused_conn_id=None):
    return Connection(conn_id=AIRBYTE_CONN_ID, conn_type='http', host='test:8001/')


class TestAirbyteTriggerSyncOp(unittest.TestCase):
    """Test get, post and raise_for_status"""

    @mock.patch('plugin.hook.AirbyteHook.submit_job')
    @mock.patch('plugin.hook.AirbyteHook.wait_for_job', return_value=None)
    def test_execute(self, mock_wait_for_job, mock_submit_job):


        mock_submit_job.return_value = mock.Mock(**{
            'json.return_value': {'job': {'id': JOB_ID}}}
        )
        op = AirbyteTriggerSyncOperator(
                task_id='test_Airbyte_op',
                airbyte_conn_id=AIRBYTE_CONN_ID,
                connection_id=AIRBYTE_CONNECTION
            )
        op.execute({})
    
        mock_submit_job.assert_called_once_with(
            connection_id=AIRBYTE_CONNECTION
        )

        mock_wait_for_job.assert_called_once_with(
            job_id=JOB_ID,
            timeout=TIMEOUT
        )
