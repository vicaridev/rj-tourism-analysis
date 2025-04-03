#%%
import requests
from dotenv import load_dotenv
import pandas as pd
import os
from serpapi import GoogleSearch
from utils.config import BASE_DIR

env_path = os.path.join(BASE_DIR, 'config', '.env')
load_dotenv(env_path)


def get_flights(payload):
    departure_id, arrival_id, outbound_date, return_date = payload.values()
    api_payload = {
        "engine": "google_flights",
        "departure_id": departure_id,
        "arrival_id": arrival_id,
        "outbound_date": outbound_date,
        "return_date": return_date,
        "currency": "BRL",
        "hl": "BR",
        "gl": "br",
        "api_key": os.getenv('SERPAPI_API_KEY')
    }
    
    search = GoogleSearch(api_payload)
    results = search.get_dict()
    
    return results