import uuid
from sqlalchemy import Column, Integer, String, Float, ARRAY, ForeignKey, DateTime
from db.models.base import Base

class Weather(Base):
    __tablename__ = 'weather'
    __table_args__={'schema': 'silver'}

    
    id = Column(String, primary_key=True, nullable=False, default=lambda: str(uuid.uuid4()))
    neighbourhood = Column(String)
    date = Column(DateTime)
    temp = Column(Float)
    feels_like = Column(Float)
    temp_min = Column(Float)
    temp_max = Column(Float)
    pressure = Column(Integer)
    sea_level = Column(Integer)
    grnd_level = Column(Integer)
    humidity = Column(Integer)
    temp_kf = Column(Float)
    clouds_perc = Column(Integer)
    rain_in_3h = Column(Float)
    weather = Column(String)
