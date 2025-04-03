from db.db_config import get_engine
from db.models.base import Base
import db.models
from utils.logger import logging

logger = logging.getLogger(__name__)
engine = get_engine()

def create_tables():
    logger.info('Creating database tables...')
    Base.metadata.create_all(engine)
    logger.info('Tables created successfully')
    
create_tables()