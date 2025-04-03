import uuid
from sqlalchemy import Column, Integer, String, Float, ARRAY, ForeignKey, DateTime
from db.models.base import Base


class Flights(Base):
    __tablename__ = 'flights'
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    duration = Column(Integer)
    airplane = Column(String)
    airline = Column(String)
    airline_logo = Column(String)
    travel_class = Column(String)
    flight_number = Column(String)
    legroom_in_cm = Column(Integer)
    departure_airport_name = Column(String)
    departure_airport_id = Column(String)
    departure_airport_time = Column(DateTime)
    arrival_airport_name = Column(String)
    arrival_airport_id = Column(String)
    arrival_airport_time = Column(DateTime)