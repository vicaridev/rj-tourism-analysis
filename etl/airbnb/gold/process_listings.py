#%%
import pandas as pd
import os
from utils.config import SILVER_DIR, GOLD_DIR, BRONZE_DIR



#%%
def process_listings_gold():
    listings_path = os.path.join(SILVER_DIR, 'listings_clean.csv')
    logger.info('Starting airbnb calendar data processing')
    df = pd.read_parquet(listings_path)
    
    mean_price_by_neighborhood = (df.groupby(['neighbourhood', 'room_type'])
                                            .agg(
                                                total_listings=('listing_id', 'nunique'),
                                                mean_price_USD=('price_USD', 'mean'),
                                                mean_price_BRL=('price_BRL', 'mean'),
                                            ).round(decimals=2)
                                            .reset_index())
        
    mean_price_by_neighborhood.to_csv(os.path.join(GOLD_DIR, 'mean_price_by_neighborhood.csv'), index=False)
    
    return

#%%
listings_path = os.path.join(SILVER_DIR, 'listings_clean.parquet')
# logger.info('Starting airbnb calendar data processing')
df = pd.read_parquet(listings_path)
#%%
df.info()

