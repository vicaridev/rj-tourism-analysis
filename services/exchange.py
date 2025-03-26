import os
import requests

def get_exchange_rate():
    api_key = os.getenv('EXCHANGE_API_KEY')
    url = f'https://v6.exchangerate-api.com/v6/{api_key}/pair/USD/BRL'
    response = requests.get(url)
    data = response.json()
    return data['conversion_rate']
