import json
from airflow.models import BaseOperator
import logging
from string import Template

import requests

cost_scenario_template = Template("""{
    "costScenario": {
        "scenarioConfig": {
            "estimateDuration": "$estimate_duration",
        },
        "workloads": [
            {
                "name": "vm-example",
                "computeVmWorkload": {
                    "instancesRunning": {
                        "usageRateTimeline": {
                            "usageRateTimelineEntries": [
                                {
                                    "usageRate": 5
                                }
                            ]
                        }
                    },
                    "machineType": {
                        "customMachineType": {
                            "machineSeries": "n1",
                            "virtualCpuCount": 4,
                            "memorySizeGb": 4
                        }
                    },
                    "region": "$region",
                }
            }
        ]
    }
}""")


def get_cost_estimate(api_access_token, region, estimate_duration):
    post_url = 'https://cloudbilling.googleapis.com/v1beta:estimateCostScenario?key={}'.format(
        api_access_token)

    cost_scenario = cost_scenario_template.substitute(
        estimate_duration=estimate_duration,
        region=region
    )

    # print(cost_scenario)

    r = requests.post(post_url, data=cost_scenario, headers={
                      'Accept': 'application/json',
                      'Content-Type': 'application/json'
                      })
    return r.json()


class RegionSelectorOperator(BaseOperator):
    def __init__(self, cas_url, *args, **kwargs):
        self.cas_url = cas_url
        super(RegionSelectorOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        logging.info("RegionSelectorOperator: cas_url: %s", self.cas_url)

        # TODO: Implement region selection logic

        selected_region = 'eastus'
        region_selection_report = {
            'cas_url': self.cas_url,
            'selected_region': selected_region
        }
        context['ti'].xcom_push(
            key='region_selection_report', value=region_selection_report)
        logging.info(
            'RegionSelectorOperator: selected_region = %s', selected_region)
        return region_selection_report
