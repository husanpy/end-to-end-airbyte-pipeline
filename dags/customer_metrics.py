from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.fraud.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
from airflow.models.baseoperator import chain


AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW = '2af44fc4-0640-4ceb-a538-c7887009d724'
AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW = 'c884a297-29c4-48fd-a2fa-ada3f63b841f'
AIRBYTE_JOB_ID_WRITE_TO_STAGING = '9cc7cf5d-8ebe-468d-80b7-62739eefed60'

@dag(
    start_date=datetime(2024, 1, 1), 
    schedule='@daily', 
    catchup=False, 
    tags=['airbyte', 'risk'],
)
def customer_metrics():

    load_customer_transactions_raw = AirbyteTriggerSyncOperator(
        task_id='load_customer_transactions_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW,
    )

    load_labeled_transactions_raw = AirbyteTriggerSyncOperator(
        task_id='load_labeled_transactions_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW,
    )

    write_to_staging = AirbyteTriggerSyncOperator(
        task_id='write_to_staging',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_WRITE_TO_STAGING,
    )

    @task
    def airbyte_jobs_done():
        return True
    
    @task.external_python(python='opt/airflow/soda_venv/bin/python')
    def audit_customer_transactions(
        scan_name='customer_transactions',
        checks_subpath='tables',
        data_source='staging'):
        from include.soda.helpers import check
        check(scan_name, checks_subpath, data_source)

    @task.external_python(python='opt/airflow/soda_venv/bin/python')
    def audit_labeled_transactions(
        scan_name='labeled_transactions',
        checks_subpath='tables',
        data_source='staging'):
        from include.soda.helpers import check
        check(scan_name, checks_subpath, data_source)

    @task
    def quality_checks_done():
        return True
    
    publish = DbtTaskGroup(
        group_id='publish',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models']
        )
    )

    chain(
        [load_customer_transactions_raw, load_labeled_transactions_raw],
        write_to_staging,
        airbyte_jobs_done(),
        [audit_customer_transactions(), audit_labeled_transactions()],
        quality_checks_done(),
        publish
    )


customer_metrics()