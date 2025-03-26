#%%
import os
import sys
import pandas as pd
import requests
import logging
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(BASE_DIR)
load_dotenv(os.path.join(BASE_DIR,"config", ".env"))
logger = logging.getLogger(__name__)

from services.exchange import get_exchange_rate

BRONZE_DIR = os.path.join(BASE_DIR, "data", "airbnb", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "airbnb", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "airbnb", "gold")

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)

listings_path = os.path.join(BASE_DIR, 'data', 'airbnb', 'bronze', 'listings.csv')
calendar_path = os.path.join(BASE_DIR, 'data', 'airbnb', 'bronze', 'calendar.csv')
reviews_path = os.path.join(BASE_DIR, 'data', 'airbnb', 'bronze', 'reviews.csv')


def transform_listings_silver(df):
    logger.info('Starting airbnb listings data transforming')
    #Silver layer
    df = (df.dropna(axis=1)
            .reset_index())
    
    df = (df.drop(columns=['scrape_id', 'last_scraped', 'source', 'picture_url', 
                                        'amenities', 'minimum_nights', 
                                        'maximum_nights', 'minimum_minimum_nights', 'maximum_minimum_nights',
                                        'minimum_maximum_nights', 'maximum_maximum_nights', 'calendar_last_scraped',
                                        'index', 'listing_url', 'host_url', 'number_of_reviews_ltm', 'number_of_reviews_l30d'
                                        ])
                                        .rename(columns={'neighbourhood_cleansed': 'neighbourhood'})
                                        )
    df['instant_bookable'] = df['instant_bookable'].replace({'f': 'No', 't': 'Yes'})
    
    logger.info('Saving airbnb listings data into csv file')
    df.to_csv(os.path.join(SILVER_DIR, 'listings_clean.csv'), index=False)
    logger.info('Airbnb listings data transformed and saved successfully!')
    return df

def transform_reviews_silver(df):
    logger.info('Starting airbnb reviews data transforming')
    #Silver layer
    df = (df.dropna()
            .reset_index(drop=True))
    
    logger.info('Saving airbnb reviews data into csv file')
    df.to_csv(os.path.join(SILVER_DIR, 'reviews_clean.csv'), index=False)
    logger.info('Airbnb reviews data transformed and saved successfully!')
    return df

#%%
def transform_calendar_silver():    
    df = pd.read_csv(calendar_path)
    logger.info('Starting airbnb calendar data transforming')
    #Silver layer
    df = (df.sort_values(['date'], ascending=False)
            .drop(columns=['adjusted_price'])
            .dropna()
            .rename(columns={'price': 'price_USD'})
            .reset_index(drop=True))
    
    df['available'] = df['available'].replace({'f': 'No', 't': 'Yes'})
    
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df[['minimum_nights', 'maximum_nights']] = df[['minimum_nights', 'maximum_nights']].astype(int)
    
    df['price_USD'] = df['price_USD'].str.replace(r'[$,]', '', regex=True)
    df['price_USD'] = df['price_USD'].astype(float)
    
    price_condition = df['price_USD'] > 0
    df = df[price_condition].copy()
    
    logger.info('Saving airbnb calendar data into csv file')
    df.to_csv(os.path.join(SILVER_DIR, 'calendar_clean.csv'), index=False)

    logger.info('Airbnb calendar data transformed and saved successfully!')
    return df

transform_calendar_silver()



#%%
def transform_calendar_gold(df):
    conversion_rate = get_exchange_rate()
    df['price_BRL'] = (df['price_USD'] * conversion_rate).round(decimals=2)
    df_bookings_price = df.drop_duplicates(subset=['listing_id'], keep='last')
    
    sorted_df_calendar = df_bookings_price[['listing_id', 'date', 'price_USD', 'price_BRL', 'available', 'minimum_nights', 'maximum_nights']]
    df_listings_with_price = (df_listings.merge(sorted_df_calendar, how='inner', left_on='id', right_on='listing_id'))
    df_listings_with_price['price_category'] = pd.cut(
    df_listings_with_price['price_USD'],
    bins=[0, 100, 300, 500, float('inf')],
    labels=['Cheap', 'Medium', 'Expensive', 'Luxury'])
    

    df_listings_with_price.to_csv(os.path.join(GOLD_DIR, 'listings_with_price.csv'), index=False)
    
    
    today = pd.Timestamp(datetime.today()).normalize()
    
    date_condition = df['available'] == 'Yes'
    df_available_bookings = df[date_condition]
    
    availability_condition = df_available_bookings['date'] >= today 
    df_available_bookings = df_available_bookings[availability_condition]
    
    df_available_bookings.to_csv(os.path.join(GOLD_DIR, 'available_bookings.csv'), index=False)


    
    mean_price_by_neighborhood = (df_listings_with_price.groupby(['neighbourhood', 'room_type'])
                                        .agg(
                                            total_listings=('listing_id', 'nunique'),
                                            mean_price_USD=('price_USD', 'mean'),
                                            mean_price_BRL=('price_BRL', 'mean'),
                                        ).round(decimals=2)
                                        .reset_index())
    
    mean_price_by_neighborhood.to_csv(os.path.join(GOLD_DIR, 'mean_price_by_neighborhood.csv'), index=False)
    
    five_days_ahead = today + timedelta(days=5)
    five_days_availability = df_available_bookings['date'] >= five_days_ahead
    next_five_days_availability = df_available_bookings[five_days_availability]
    
    next_five_days_availability.to_csv(os.path.join(GOLD_DIR, 'next_five_days_listings_availability'), index=False)


#%%
def transform_airbnb_data_silver():    
    df_listings = pd.read_csv(listings_path)
    df_calendar = pd.read_csv(calendar_path)
    df_reviews = pd.read_csv(reviews_path)
    
    
    transform_listings_silver(df_listings)
    transform_reviews_silver(df_reviews)
    transform_calendar_silver(df_calendar)
    
    logger.info('All transforming executed successfully')
    return

#%%
transform_airbnb_data()
# #%%
# today = pd.Timestamp(datetime.today()).normalize()
# today

# #%%
# calendar_path = os.path.join(BASE_DIR, 'data', 'airbnb', 'bronze', 'calendar.csv')
# df_calendar = pd.read_csv(calendar_path)
# df_calendar

# #%%
# today = pd.Timestamp(datetime.today()).normalize()
# df_calendar['date'] = pd.to_datetime(df_calendar['date'])
# date_range = pd.date_range(today, periods=5)

# next_five_days_availability = df_calendar['date'].isin(date_range)

# #%%
# next_five_days = df_calendar[next_five_days_availability]
# next_five_days.reset_index()
# grouped = next_five_days.groupby(['date'])

# grouped.head()


# #%%
# listings_path = os.path.join(BASE_DIR, 'data', 'airbnb', 'bronze', 'listings.csv')
# df_listings = pd.read_csv(listings_path)
# df_listings[['latitude', 'longitude']]


#%%
df = pd.read_csv(calendar_path)

#%%
