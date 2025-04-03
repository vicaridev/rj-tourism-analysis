#%%
import os
import pandas as pd
import logging
from services.exchange import get_exchange_rate
import numpy as np
from utils.config import SILVER_DIR, BRONZE_DIR


logger = logging.getLogger(__name__)

def transform_listings_silver(): 
    logger.info('Starting airbnb listings data transforming...')
    listings_path = os.path.join(BRONZE_DIR, 'listings.parquet')
    df = pd.read_parquet(listings_path)
    
    df['host_id_2'] = df['host_id']

    hosts_columns = ['host_id',
                    'host_url', 'host_name', 'host_since', 'host_location', 'host_about',
                    'host_response_time', 'host_response_rate', 'host_acceptance_rate',
                    'host_is_superhost', 'host_thumbnail_url', 'host_picture_url',
                    'host_neighbourhood', 'host_listings_count',
                    'host_total_listings_count', 'host_verifications',
                    'host_has_profile_pic', 'host_identity_verified']

    df_hosts = df[hosts_columns].copy()

    to_replace_hosts_columns = ['host_is_superhost', 'host_has_profile_pic', 'host_identity_verified']
    df_hosts[to_replace_hosts_columns] = df_hosts[to_replace_hosts_columns].replace({'f': 'No', 't': 'Yes'})

    to_replace_hosts_columns = ['host_response_rate', 'host_acceptance_rate']
    df_hosts[to_replace_hosts_columns] = df_hosts[to_replace_hosts_columns].replace('%', '', regex=True)

    df_hosts = (df_hosts.dropna(subset=['host_name', 'host_location', 'host_url', 'host_verifications', 'host_identity_verified'])
                    .fillna({'host_response_time': 'Unknown',
                            'host_response_rate': 0,
                            'host_acceptance_rate': 0,
                            'host_is_superhost': 'Unknown',
                            'host_about': 'Unknown',
                            'host_neighbourhood': 'Unknown'
                            })
                    .astype({
                            'host_id': 'string',
                            'host_url': 'string',
                            'host_name': 'string',
                            'host_since': 'datetime64[ns]',
                            'host_location': 'string',
                            'host_about': 'string',
                            'host_response_time': 'string',
                            'host_response_rate': 'int',
                            'host_acceptance_rate': 'int',
                            'host_is_superhost': 'string',
                            'host_thumbnail_url': 'string',
                            'host_picture_url': 'string',
                            'host_neighbourhood': 'string',
                            'host_listings_count': 'int',
                            'host_total_listings_count': 'int',
                            'host_has_profile_pic': 'string',
                            'host_identity_verified': 'string'
                    })
                    .rename(columns={'host_response_rate': 'host_response_rate_perc',
                                    'host_acceptance_rate': 'host_acceptance_rate_perc',
                                    'host_id': 'id'})
                    )
    
    logger.info('Saving airbnb hosts data into parquet file...')

    df_hosts.to_parquet(os.path.join(SILVER_DIR, 'hosts_clean.parquet'), index=False)

    df = (df.drop(columns=hosts_columns + ['scrape_id', 'last_scraped', 'source', 
                                    'amenities', 'minimum_minimum_nights', 'maximum_minimum_nights',
                                    'minimum_maximum_nights', 'maximum_maximum_nights', 'calendar_last_scraped',
                                    'number_of_reviews_ltm', 'number_of_reviews_l30d', 'calendar_updated', 'neighbourhood',
                                    'neighbourhood_group_cleansed', 'license', 'review_scores_accuracy', 'review_scores_cleanliness',
                                    'review_scores_checkin', 'review_scores_communication', 'review_scores_location', 'review_scores_value'])
            .rename(columns={'price': 'price_USD',
                             'host_id_2': 'host_id'})
            .dropna(subset=['price_USD', 'has_availability', 'bathrooms', 'bathrooms_text', 'bedrooms', 'beds'])
            .astype({'id': 'string',
                     'available': 'string'})
            )

    df['price_USD'] = (df['price_USD'].str.replace(r'[$,]', '', regex=True)
                                    .astype(float))


    price_condition = df['price_USD'] > 0
    df = df[price_condition].copy()

    conversion_rate = get_exchange_rate()

    df['price_BRL'] = (df['price_USD'] * conversion_rate).round(decimals=2)
    df['price_category'] = pd.cut(df['price_USD'],
                                    bins=[0, 100, 300, 500, float('inf')],
                                    labels=['Cheap', 'Medium', 'Expensive', 'Luxury'])


    df = (df.rename(columns={'neighbourhood_cleansed': 'neighbourhood'})
            .astype({
                    'listing_url': 'string',
                    'name': 'string',
                    'description': 'string',
                    'neighborhood_overview': 'string',
                    'picture_url': 'string',
                    'neighbourhood': 'string',
                    'property_type': 'string',
                    'room_type': 'string',
                    'bathrooms_text': 'string',
                    'has_availability': 'string',
                    'first_review': 'datetime64[ns]',
                    'last_review': 'datetime64[ns]',
                    'instant_bookable': 'string',}))
    
    df['first_review_filled'] = df['first_review'].fillna(pd.NaT)
    df['never_reviewd'] = df['first_review'].isna().astype(int)
    df = df.replace({
            'description': {np.nan: 'Unknown'},
            'neighborhood_overview': {np.nan: 'Unknown'},
            'reviews_per_month': {np.nan: 0}})

    df[['instant_bookable', 'has_availability']] = df[['instant_bookable', 'has_availability']].replace({'f': 'No', 't': 'Yes'})

    logger.info('Saving airbnb listings data into parquet file...')

    df.to_parquet(os.path.join(SILVER_DIR, 'listings_clean.parquet'), index=False)


    logger.info('Airbnb listings data transformed and saved successfully!')

    return
