import requests

class GQLAPI:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()

    def execute(self, query, variables:None):
        payload = {'query': query, 'variables': variables}
        response = self.session.post(self.url, json=payload)
        response.raise_for_status()
        return response.json()
