from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.bash import BashOperator

import logging

class DataCollectorBuild(BashOperator):
    def __init__(self, region_selector_id, *args, **kwargs):
        self.region_selector_id = region_selector_id
        super(DataCollectorBuild, self).__init__(bash_command='', *args, **kwargs)

    def execute(self, context):
        region_selection_report = context['ti'].xcom_pull(key='region_selection_report', task_ids=self.region_selector_id)
        logging.info("DataCollector: selected_region: %s", region_selection_report['selected_region'])
        bash_cmd = 'echo "data_collector_build: TODO: replace this with gcloud command for %s"' % region_selection_report['selected_region']
        self.bash_command = bash_cmd
        logging.info("DataCollector: bash_cmd: %s", bash_cmd)
        super(DataCollectorBuild, self).execute(context)
        return "DataCollectorBuild: execute()"

class DataCollectorBuildSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(DataCollectorBuildSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info("DataCollectorBuildSensor: poke()")
        return True

class DataCollector(BaseOperator):
    def __init__(self, customer_id, *args, **kwargs):
        self.customer_id = customer_id
        super(DataCollector, self).__init__(*args, **kwargs)

    def execute(self, context):
        logging.info("DataCollector: execute() for customer_id: %s", self.customer_id)
        return "DataCollector: execute()"

class DataCollectionSensor(BaseSensorOperator):
    def __init__(self, customer_id, *args, **kwargs):
        self.customer_id = customer_id
        super(DataCollectionSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info("DataCollectionSensor: poke() for customer_id: %s", self.customer_id)
        return True

class ReportProcessBuild(BashOperator):
    def __init__(self, region_selector_id, *args, **kwargs):
        self.region_selector_id = region_selector_id
        super(ReportProcessBuild, self).__init__(bash_command='', *args, **kwargs)

    def execute(self, context):
        region_selection_report = context['ti'].xcom_pull(key='region_selection_report', task_ids=self.region_selector_id)
        logging.info("ReportProcessBuild: selected_region: %s", region_selection_report['selected_region'])
        bash_cmd = 'echo "report_process_build: TODO: replace this with gcloud command for %s"' % region_selection_report['selected_region']
        self.bash_command = bash_cmd
        logging.info("ReportProcessBuild: bash_cmd: %s", bash_cmd)
        super(ReportProcessBuild, self).execute(context)
        return "ReportProcessBuild: execute()"

class ReportProcessBuildSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(ReportProcessBuildSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info("ReportProcessBuildSensor: poke()")
        return True

class ReportProcessor(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(ReportProcessor, self).__init__(*args, **kwargs)

    def execute(self, context):
        logging.info("ReportProcessor: execute()")
        return "ReportProcessor: execute()"

class ReportProcessorReadySensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(ReportProcessorReadySensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info("ReportProcessorReadySensor: poke()")
        return True
