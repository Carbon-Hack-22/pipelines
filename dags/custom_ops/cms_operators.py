from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.bash import BashOperator

import logging

class ReportSiteGeneratorBuild(BashOperator):
    def __init__(self, region_selector_id, *args, **kwargs):
        self.region_selector_id = region_selector_id
        super(ReportSiteGeneratorBuild, self).__init__(bash_command='', *args, **kwargs)

    def execute(self, context):
        region_selection_report = context['ti'].xcom_pull(key='region_selection_report', task_ids=self.region_selector_id)
        logging.info("ReportSiteGeneratorBuild: selected_region: %s", region_selection_report['selected_region'])
        bash_cmd = 'echo "report_site_generator_build: TODO: replace this with gcloud command for %s"' % region_selection_report['selected_region']
        self.bash_command = bash_cmd
        logging.info("ReportSiteGeneratorBuild: bash_cmd: %s", bash_cmd)
        super(ReportSiteGeneratorBuild, self).execute(context)
        return "ReportSiteGeneratorBuild: execute()"


class ReportSiteGeneratorBuildSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(ReportSiteGeneratorBuildSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info("ReportSiteGeneratorBuildSensor: poke()")
        return True


class ReportSiteGenerator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(ReportSiteGenerator, self).__init__(*args, **kwargs)

    def execute(self, context):
        logging.info("ReportSiteGenerator: execute()")
        return "ReportSiteGenerator: execute()"


class ReportSiteReadySensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(ReportSiteReadySensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info("ReportSiteReadySensor: poke()")
        return True