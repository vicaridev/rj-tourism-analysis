import pandas as pd
import os
from datetime import datetime

def transform_calendar_gold():
    listings_path = os.path.join(BASE_DIR, 'data', 'airbnb', 'silver', 'listings_clean.csv')
    df_listings = pd.read_csv(listings_path)
    today = pd.Timestamp(datetime.today()).normalize()
    
    date_condition = df_available_bookings['date'] >= today 
    df_available_bookings = df[date_condition]
    
    availability_condition = df['available'] == 'Yes'
    df_available_bookings = df_available_bookings[availability_condition]
    
    df_available_bookings.to_csv(os.path.join(GOLD_DIR, 'available_bookings.csv'), index=False)

    logger.info('Calendar at gold layer processed successfully')
    
    return 