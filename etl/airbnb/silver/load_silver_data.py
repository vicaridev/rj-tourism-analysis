from db.db_utils import get_engine
import pandas as pd
from utils.config import SILVER_DIR
from sqlalchemy.orm import sessionmaker
from sqlalchemy import insert
from utils.logger import logging
import os

from db.models.airbnb.calendar_models import Calendar
from db.models.airbnb.listings_models import Listing
from db.models.airbnb.hosts_models import Host
from db.models.airbnb.neighbourhood_models import Neighbourhood
from db.models.airbnb.reviews_models import Reviews

CHUNK_SIZE = 10000


def load_airbnb_silver():
    try:
        model_mapping = {
            # 'hosts_clean.parquet': Host,
            # 'listings_clean.parquet': Listing,
            # 'neighbourhood_geolocation.parquet': Neighbourhood,
            'calendar_clean.parquet': Calendar,
            # 'reviews_clean.parquet': Reviews,
        }
        engine = get_engine()
        SessionLocal = sessionmaker(bind=engine)
        for filename, model_class in model_mapping.items():
            try:
                file_path = os.path.join(SILVER_DIR, filename)
                df = pd.read_parquet(file_path)

                with SessionLocal.begin() as session:
                    for i in range(0, len(df), CHUNK_SIZE):
                        data = df.iloc[i:i+CHUNK_SIZE].to_dict(orient='records')
                        session.execute(insert(model_class), data)
                    logging.info(f'Data loaded on {model_class.__name__} table successfully')
            except Exception as e:
                logging.error(f'Error loading data for model {model_class.__name__}: {str(e)}', exc_info=True)
    except Exception as e:
        logging.error(f'Unexpected error in load_airbnb_silver: {str(e)}', exc_info=True)