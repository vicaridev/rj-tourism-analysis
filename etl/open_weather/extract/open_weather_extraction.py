#%%
import requests
import os
from dotenv import load_dotenv
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
load_dotenv(os.path.join(BASE_DIR, 'config', '.env'))
API_KEY = os.getenv('OPENWEATHER_API_KEY')

def get_weather_info(lat='-22.9035', lon='-43.2096'):

    URL = f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units=metric&appid={API_KEY}'


    response = requests.get(URL)
    data = response.json()


    neighbourhood = data['city']['name']
    
    df = pd.DataFrame(data['list'])
    
    df_main = pd.json_normalize(df['main'])
    df_clouds = pd.json_normalize(df['clouds'])
    df_rain = pd.json_normalize(df['rain']).fillna(0)
    df_weather = pd.json_normalize(df['weather'].apply(lambda x: x[0]))

    df_final = pd.concat([df[['dt_txt']], df_main, df_clouds, df_rain, df_weather[['main']]], axis=1)

    df_final.rename(columns={
        'dt_txt': 'date',
        'all': '%_clouds',
        'main': 'weather',
        '3h': '3h_rain'
    }, inplace=True)

    df_final.insert(0, 'id', range(1, len(df_final) + 1))
    df_final.insert(1, 'neighbourhood', neighbourhood)
    df_final.reset_index(drop=True, inplace=True)
    
    return df_final
