import requests
import json
import string
import random


SYMBOLS = string.ascii_letters + string.digits

def generate_id():
    """Generates a random sequence for use with the Wildberries API.
    """
    return ''.join(random.sample(SYMBOLS, 36))

class WbApi:
    def __init__(self, auth_token):
        self.api_url = 'https://suppliers-api.wildberries.ru'
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': auth_token,
        }
        self.data = {"jsonrpc": "2.0"}

    def product_cards(self, offset=0, limit=1000):
        """Returns a list of customer's product cards placed on Wildberries.
        'offset' can be used to iterate over large lists.
        """
        _url = f'{self.api_url}/card/list'
        _data = {
            'id': generate_id(),
            'params': {
                'query': {
                    'limit': limit,
                    'offset': offset,
                },
            },
        }
        return requests.post(
            _url,
            headers=self.headers,
            data=json.dumps(self.data | _data),
        )
