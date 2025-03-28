import psycopg2
from psycopg2.extras import DictCursor
import os
import logging


logger = logging.getLogger(__name__)

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}


def get_connection():
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
        with conn.cursos(cursorFactory=DictCursor) as cursor:
            
            cursor.execute(query, params or ())
            
            if fetch_one:
                return cursor.fetch_one()
            if fetch_all:
                return cursor.fetch_all()
            conn.commit()
            
    except Exception as e:
        logger.error(f'Couldn\'t execute query. Error: {e}')
    finally:
        conn.close()
