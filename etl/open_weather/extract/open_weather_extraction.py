#%%
import requests
import os
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
load_dotenv(os.path.join(BASE_DIR, 'config', '.env'))

API_KEY = os.getenv('OPENWEATHER_API_KEY')
URL = f'https://api.openweathermap.org/data/2.5/forecast?lat=-22.995080&lon=-43.236040&units=metric&appid={API_KEY}'


response = requests.get(URL)
data = response.json()

print(data)