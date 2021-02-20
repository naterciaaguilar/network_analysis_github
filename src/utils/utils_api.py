import requests
import json
import time

from datetime import datetime

def verify_request_time(token, request_type):
    rate_limit_request = 'https://api.github.com/rate_limit'
    rate_limit = requests.get(rate_limit_request, headers={'Authorization': 'token %s' % token})
    rate_limit = json.loads(rate_limit.content)

    if rate_limit['resources'][request_type]['remaining'] == 0:
        now = datetime.utcnow()
        reset_time = datetime.utcfromtimestamp(rate_limit['resources'][request_type]['reset'])

        wait_seconds = (reset_time - now).total_seconds()

        # wait the reset time to continue
        if wait_seconds > 0:
            print(f'\nSleeping {wait_seconds} until continue...\n')
            time.sleep(wait_seconds + 10)