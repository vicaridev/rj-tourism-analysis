#%%
import os
import pandas as pd
import logging
import numpy as np
from utils.config import SILVER_DIR, BRONZE_DIR
from utils.logger import logging

logger = logging.getLogger(__name__)


def transform_reviews_silver():
    logging.info('Transforming reviews data...')
    reviews_path = os.path.join(BRONZE_DIR, 'reviews.parquet')
    #Silver layer
    df = pd.read_parquet(reviews_path)
    df = (df.replace({
            'reviewer_name': {np.nan: 'Unknown'},
            'comments': {np.nan: 'Not commented'}
            })
            .astype({
                'date': 'datetime64[ns]',
                'reviewer_name': 'string',
                'listing_id': 'string',
                'comments': 'string',
                'reviewer_id': 'string'
            })
            .drop(columns=['id'])
            )
    
    logging.info('Saving reviews data into parquet...')
    df.to_parquet(os.path.join(SILVER_DIR, 'reviews_clean.parquet'), index=False)

    logging.info('Reviews data transformed and saved successfully!')

    return