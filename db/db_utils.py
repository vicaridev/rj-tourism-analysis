from db.db_config import get_engine
from db.models.base import Base
import db.models
from utils.logger import logging
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.orm import Session
from db.db_config import get_connection

from db.models.airbnb.reviews_models import Reviews
from db.models.airbnb.listings_models import Listing, MeanPrice
from db.models.airbnb.calendar_models import Calendar
from db.models.airbnb.hosts_models import Host
from db.models.airbnb.neighbourhood_models import Neighbourhood
from db.models.openweather.openweather_models import Weather
from db.models.google_flights.google_flights_models import Flights

logging.getLogger(__name__)
engine = get_engine()
insp = sa.inspect(engine)
session = Session(engine)

def create_tables():
    try:
        logging.info('Creating database tables manually...')

        tables = [
            Host,
            MeanPrice,
            Neighbourhood,
            Weather,
            Listing,
            Reviews,
            Calendar,
            Flights,
        ]

        for model in tables:
            logging.info(f'Creating table: {model.__tablename__}')
            model.__table__.create(engine, checkfirst=True)

        logging.info('All tables created successfully.')
    except Exception as e:
        logging.error(f'Error while creating tables: {e}')
            
def check_table_existecy(tablename, schema):
    try:
        table_exists = insp.has_tables(tablename, schema)
        return table_exists
    
    except Exception as e:
        return logging.info(f'Error to check table existence: {e}')
    
def load_data():
    try:
        
        return
    except Exception as e:
        logging.info(f'Error on loading data into database: {e}')

def create_schemas():
    try:
        with Session(engine) as session:
            for schema in ['bronze', 'silver', 'gold']:
                session.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
            session.commit()
    except Exception as e:
        logging.info(f'Error on create schemas: {e}')
        session.rollback()
        
def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    conn = get_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            
            cursor.execute(query, params or ())
            
            if fetch_one:
                return cursor.fetchone()
            if fetch_all:
                return cursor.fetchall()
            conn.commit()
            
    except Exception as e:
        logger.error(f'Couldn\'t execute query. Error: {e}')
    finally:
        conn.close()
        return
    
    
create_schemas()
create_tables()