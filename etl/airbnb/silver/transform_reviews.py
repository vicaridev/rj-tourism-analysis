#%%
import os
import pandas as pd
import logging
import numpy as np
from utils.config import SILVER_DIR, BRONZE_DIR

logger = logging.getLogger(__name__)


def transform_reviews_silver():
    logger.info('Starting airbnb reviews data transforming')
    reviews_path = os.path.join(BRONZE_DIR, 'reviews.parquet')
    #Silver layer
    df = pd.read_parquet(reviews_path)
    df = df.replace({
            'reviewer_name': {np.nan: 'Unknown'},
            'comments': {np.nan: 'Not commented'}
    })


    logger.info('Saving airbnb reviews data into csv file')
    df.to_parquet(os.path.join(SILVER_DIR, 'reviews_clean.parquet'), index=False)

    logger.info('Airbnb reviews data transformed and saved successfully!')

    return