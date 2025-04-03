#%%
import psycopg2
from psycopg2.extras import DictCursor
from pathlib import Path
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib.parse import quote_plus
import sys
from utils.config import BASE_DIR
from dotenv import load_dotenv

env_path = os.path.abspath(os.path.join(BASE_DIR, 'config', '.env.postgres'))

load_dotenv(env_path)
logger = logging.getLogger(__name__)


DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "client_encoding": "utf8"
}


def get_connection():
    try:
        conn = psycopg2.connect("dbname=airflow user=admin password=securepassword host=127.0.0.1")   
        return conn
    except Exception as e:
        logger.error(f'Couldn\'t connect to database. Error: {e}')
        return
    
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

def get_engine():
    try:
        engine = create_engine(os.getenv("POSTGRES_URL"))
        return engine
    except Exception as e:
        logger.error(f'Couldn\'t create engine. Error: {e}')
        return