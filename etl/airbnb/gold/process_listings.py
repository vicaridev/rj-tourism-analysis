#%%
import pandas as pd
import os
from utils.config import SILVER_DIR, GOLD_DIR, BRONZE_DIR
from utils.logger import logging

def process_listings_gold():
    logging.info('Processing listings data...')
    listings_path = os.path.join(SILVER_DIR, 'listings_clean.parquet')
    df = pd.read_parquet(listings_path)

    df

    mean_price_by_neighborhood = (df.groupby(['neighbourhood', 'room_type', 'price_category'], observed=True)
                                            .agg(
                                                total_listings=('id', 'nunique'),
                                                mean_price_USD=('price_USD', 'mean'),
                                                mean_price_BRL=('price_BRL', 'mean'),
                                            ).round(decimals=2)
                                            .reset_index())
        
    mean_price_by_neighborhood.to_parquet(os.path.join(GOLD_DIR, 'mean_price_by_neighborhood.parquet'), index=False)

    df_neighbourhood = df[['neighbourhood', 'latitude', 'longitude']]
    df_neighbourhood = df_neighbourhood.groupby('neighbourhood', as_index=False).agg({
            'latitude': 'mean',
            'longitude': 'mean'
    })

    logging.info('Saving neighbourhood geolocation data into parquet...')
    df_neighbourhood.to_parquet(os.path.join(GOLD_DIR, 'neighbourhood_geolocation.parquet'), index=False)
    logging.info('Neighbourhood geolocation data processed successfully!')
    return
