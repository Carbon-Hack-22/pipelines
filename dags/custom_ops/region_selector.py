from airflow.models import BaseOperator
import logging


class RegionSelectorOperator(BaseOperator):
    def __init__(self, cas_url, *args, **kwargs):
        self.cas_url = cas_url
        super(RegionSelectorOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # TODO: Implement region selection logic
        logging.info("RegionSelectorOperator: cas_url: %s", self.cas_url)

        selected_region = 'eastus'
        region_selection_report = {
            'cas_url': self.cas_url,
            'selected_region': selected_region
        }
        context['ti'].xcom_push(key='region_selection_report', value=region_selection_report)
        logging.info('RegionSelectorOperator: selected_region = %s', selected_region)
        return region_selection_report