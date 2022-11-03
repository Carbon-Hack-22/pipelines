

import unittest
from region_selector import get_cost_estimate


class RegionSelectorTest(unittest.TestCase):

    def test_get_cost_estimate(self):
        # You can get your own API access token from the Google Cloud Console, "APIs & Services > Credentials"
        # Click on "Create Credentials" and select "API Key"
        api_access_token = 'YOUR_API_ACCESS_TOKEN_HERE'
        region = 'us-central1'
        estimate_duration = '360000s'
        res_json = get_cost_estimate(api_access_token, region, estimate_duration)
        print(res_json)


if __name__ == '__main__':
    unittest.main()