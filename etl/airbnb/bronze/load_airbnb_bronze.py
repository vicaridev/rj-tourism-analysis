#%%
import pandas as pd
from utils.config import BRONZE_DIR
from utils.logger import logging
import os
from sqlalchemy import MetaData, Table, Column, String
from sqlalchemy import insert
from sqlalchemy.orm import sessionmaker
from db.db_config import get_engine

SCHEMA = 'bronze'
CHUNK_SIZE = 10000

def load_airbnb_bronze():
    try:
        files = {
        'reviews.parquet': 'reviews',
        'calendar.parquet': 'calendar',
        'listings.parquet': 'listings'
    }
        engine = get_engine()
        SessionLocal = sessionmaker(bind=engine)
        for filename, tablename in files.items():
            file_path = os.path.join(BRONZE_DIR, filename)
            df = pd.read_parquet(file_path)
            table_name = os.path.splitext(filename)[0]
            metadata = MetaData()
            columns = [Column(col, String) for col in df.columns]
            
            bronze_table = Table(
                table_name,
                metadata,
                *columns,
                schema=SCHEMA
            )
            metadata.create_all(engine, checkfirst=True)
            with SessionLocal.begin() as session:
                for i in range(0, len(df), CHUNK_SIZE):
                    data = df.iloc[i:i+CHUNK_SIZE].astype(str).to_dict(orient='records')
                    session.execute(insert(bronze_table), data)
        pass
    except Exception as e:
        logging.error(f'Error on load airbnb bronze data: {e}')

    pass

    
    