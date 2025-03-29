#%%
import pandas as pd
import sys

def transform_flights_data(data):
    df_best_flights = pd.json_normalize(data['best_flights'])
    flights_list = df_best_flights['flights'].explode().tolist()
    df_flights = pd.json_normalize(flights_list)
    df_flights['legroom'] = (df_flights['legroom'].replace(r'cm', '', regex=True)
                                                    .astype(str)
                                                    .strip())
    df_flights = (df_flights.drop(columns=['extensions'])
                            .rename(columns={'departure_airport.name': 'departure_airport_name',
                                            'departure_airport.id': 'departure_airport_id',
                                            'departure_airport.time': 'departure_airport_time',
                                            'arrival_airport.name': 'arrival_airport_name',
                                            'arrival_airport.id': 'arrival_airport_id',
                                            'arrival_airport.time': 'arrival_airport_time'
                                            })
                            .astype({
                                    'airplane': 'string',
                                    'airline': 'string',
                                    'airline_logo': 'string',
                                    'travel_class': 'string',
                                    'flight_number': 'string',
                                    'legroom': 'int',
                                    'departure_airport_name': 'string',
                                    'departure_airport_id': 'string',
                                    'departure_airport_time': 'datetime64[ns]',
                                    'arrival_airport_name': 'string',
                                    'arrival_airport_id': 'string',
                                    'arrival_airport_time': 'datetime64[ns]',
                                    }))

    return df_flights