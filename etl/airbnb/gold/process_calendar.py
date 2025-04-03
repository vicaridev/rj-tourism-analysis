#%%
import pandas as pd
import os
from datetime import datetime
from utils.config import BASE_DIR, SILVER_DIR

def transform_calendar_gold():
    calendar_path = os.path.join(BASE_DIR, 'data', 'airbnb', 'silver', 'calendar_clean.parquet')
    df_calendar = pd.read_parquet(calendar_path)
    today = pd.Timestamp(datetime.today()).normalize()

    availability_condition = df_calendar['available'] == 'Yes'
    df_available_bookings = df_calendar[availability_condition]

    date_condition = df_available_bookings['date'] >= today
    df_available_bookings = df_available_bookings[date_condition]


    df_available_bookings.to_csv(os.path.join(GOLD_DIR, 'available_bookings.csv'), index=False)

    logger.info('Calendar at gold layer processed successfully')
        
    return