
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.models import Variable
from custom_ops.region_selector import RegionSelectorOperator
from custom_ops.etl_operators import DataCollectorBuild, DataCollectorBuildSensor, DataCollector, DataCollectionSensor 
from custom_ops.etl_operators import ReportProcessBuild, ReportProcessBuildSensor, ReportProcessor, ReportProcessorReadySensor
from custom_ops.cms_operators import ReportSiteGeneratorBuild, ReportSiteGeneratorBuildSensor, ReportSiteGenerator, ReportSiteReadySensor

global_vars = Variable.get("global_config", deserialize_json=True)

customers = ['customer1', 'customer2', 'customer3']

with DAG(
    'carbon_report',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': [global_vars['admin_email']],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='Carbon Report ETL & CMS Workflow',
    schedule=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['carbon_report'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    cas_task_id = 'cas_task'
    cas_task = RegionSelectorOperator(task_id=cas_task_id, cas_url=global_vars['cas_url'])

    start >>  cas_task

    report_site_generator = ReportSiteGenerator(task_id='report_site_generator')
    report_site_ready_sensor = ReportSiteReadySensor(task_id='report_site_ready_sensor')

    report_site_generator >> report_site_ready_sensor >> end

    report_processor = ReportProcessor(task_id='report_processor')
    wait_for_report_processor = ReportProcessorReadySensor(task_id='wait_for_report_processor')

    report_processor >> wait_for_report_processor >> report_site_generator

    data_collector_build = DataCollectorBuild(region_selector_id=cas_task_id, task_id='data_collector_build')
    wait_for_data_collector_build = DataCollectorBuildSensor(task_id='wait_for_data_collector_build')

    cas_task >> data_collector_build >> wait_for_data_collector_build

    report_process_build = ReportProcessBuild(region_selector_id=cas_task_id, task_id='report_process_build')
    wait_for_report_process_build = ReportProcessBuildSensor(task_id='wait_for_report_process_build')

    cas_task >> report_process_build >> wait_for_report_process_build >> report_processor

    report_site_generator_build = ReportSiteGeneratorBuild(region_selector_id=cas_task_id, task_id='report_site_generator_build')
    wait_for_report_site_generator_build = ReportSiteGeneratorBuildSensor(task_id='wait_for_report_site_generator_build')

    cas_task >> report_site_generator_build >> wait_for_report_site_generator_build >> report_site_generator

    for customer in customers:
        data_collection = DataCollector(    customer_id=customer, task_id=f'data_collection_{customer}')
        wait_for_data_collection = DataCollectionSensor(customer_id=customer, task_id=f'wait_for_data_collection_{customer}')
        wait_for_data_collector_build >> data_collection >> wait_for_data_collection >> report_processor



