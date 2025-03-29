#%%
import requests
from dotenv import load_dotenv
import pandas as pd
from typing import TypedDict
import os
from serpapi import GoogleSearch

load_dotenv('../../../config/.env')

class get_flight_payload(TypedDict):
    departure_id: str
    arrival_id: str
    outbound_date: str
    return_date: str


def get_flights():
    # departure_id, arrival_id, outbound_date, return_date = payload.values()
    api_payload = {
        "engine": "google_flights",
        "departure_id": 'VCP',
        "arrival_id": 'SDU',
        "outbound_date": '2025-03-29',
        "return_date": '2025-05-03',
        "currency": "BRL",
        "hl": "BR",
        "gl": "br",
        "api_key": os.getenv('SERPAPI_API_KEY')
    }
    
    search = GoogleSearch(api_payload)
    results = search.get_dict()
    
    return results