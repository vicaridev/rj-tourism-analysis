#%%
import os
import pandas as pd
import logging
from datetime import date, datetime
from utils.config import SILVER_DIR, BRONZE_DIR

logger = logging.getLogger(__name__)


def transform_calendar_silver():
    
    logger.info('Starting airbnb calendar data transforming')
    calendar_path = os.path.join(BRONZE_DIR, 'calendar.csv')    
    df = pd.read_csv(calendar_path)

    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    today = pd.Timestamp(datetime.today()).normalize()
    date_condition = df['date'] >= today
    df = df[date_condition]

    df = (df.sort_values(['date'], ascending=False)
            .drop(columns=['adjusted_price'])
            .rename(columns={'price': 'price_USD'}))

    df['price_USD'] = (df['price_USD'].replace(r'[,$]','', regex=True)
                                    .astype(float))

    df['available'] = (df['available'].replace({'f': 'No', 't': 'Yes'})
                                    .astype(str))

    logger.info('Saving airbnb calendar data into csv file')
    df.to_csv(os.path.join(SILVER_DIR, 'calendar_clean.csv'), index=False)

    logger.info('Airbnb calendar data transformed and saved successfully!')
    
    return