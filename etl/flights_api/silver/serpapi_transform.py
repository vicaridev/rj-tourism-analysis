#%%
import pandas as pd
import sys
from etl.flights_api.bronze.serpapi_extraction import get_flights
import os
from utils.config import SILVER_DIR

def transform_flights_data(data):
    df_best_flights = pd.json_normalize(data['best_flights'])
    df_best_flights
    flights_list = df_best_flights['flights'].explode().tolist()
    flights_list
    df_flights = pd.json_normalize(flights_list)
    df_flights
    df_flights['legroom'] = (df_flights['legroom'].replace(r'cm', '', regex=True)
                                                    .astype(str))
    df_flights = (df_flights.drop(columns=['extensions'])
                            .rename(columns={'departure_airport.name': 'departure_airport_name',
                                            'departure_airport.id': 'departure_airport_id',
                                            'departure_airport.time': 'departure_airport_time',
                                            'arrival_airport.name': 'arrival_airport_name',
                                            'arrival_airport.id': 'arrival_airport_id',
                                            'arrival_airport.time': 'arrival_airport_time',
                                            'legroom': 'legroom_in_cm',
                                            'duration': 'duration_minutes'
                                            })
                            .astype({
                                    'airplane': 'string',
                                    'airline': 'string',
                                    'airline_logo': 'string',
                                    'travel_class': 'string',
                                    'flight_number': 'string',
                                    'legroom_in_cm': 'int',
                                    'departure_airport_name': 'string',
                                    'departure_airport_id': 'string',
                                    'departure_airport_time': 'datetime64[ns]',
                                    'arrival_airport_name': 'string',
                                    'arrival_airport_id': 'string',
                                    'arrival_airport_time': 'datetime64[ns]',
                                    }))
    df_flights.to_parquet(os.path.join(SILVER_DIR, 'flights_cleant.parquet'))

    # return