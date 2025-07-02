#%%
import os
import requests
from dotenv import load_dotenv
from utils.config import BASE_DIR

load_dotenv(os.path.join(BASE_DIR, 'config', '.env'))

def get_exchange_rate():
    try:    
        api_key = os.getenv('EXCHANGE_API_KEY')
        url = f'https://v6.exchangerate-api.com/v6/{api_key}/pair/USD/BRL'
        response = requests.get(url)
        data = response.json()
        return data['conversion_rate']
    except Exception as e:
        return print(f"Error: {e}")