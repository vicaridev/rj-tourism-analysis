
#%%
import psycopg2
from psycopg2.extras import DictCursor
from pathlib import Path
import os
import logging
import sys
from dotenv import load_dotenv, dotenv_values

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
sys.path.append(BASE_DIR)

env_path = os.path.abspath(os.path.join(BASE_DIR, '..', 'config', '.env.postgres'))

load_dotenv(env_path)
logger = logging.getLogger(__name__)


def get_connection():
    DB_PARAMS = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSRGRES_HOST"),
        "port": os.getenv("POSRGRES_PORT"),
    }
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logger.error(f'Couldn\'t connect to database. Error: {e}')
        return None
    
def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    conn = get_connection()
    if not conn:
        return None
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

