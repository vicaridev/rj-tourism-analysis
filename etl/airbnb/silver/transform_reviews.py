import os
import pandas as pd
import logging
from utils.config import SILVER_DIR, BRONZE_DIR

logger = logging.getLogger(__name__)


def transform_reviews_silver():
    logger.info('Starting airbnb reviews data transforming')
    reviews_path = os.path.join(BRONZE_DIR, 'reviews.csv')
    #Silver layer
    df = pd.read_csv(reviews_path)
    df = (df.dropna()
            .reset_index(drop=True))
    
    logger.info('Saving airbnb reviews data into csv file')
    df.to_csv(os.path.join(SILVER_DIR, 'reviews_clean.csv'), index=False)
    
    logger.info('Airbnb reviews data transformed and saved successfully!')
    return df