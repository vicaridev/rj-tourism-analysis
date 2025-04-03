import uuid
from sqlalchemy import Column, Integer, String, Float, ARRAY, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from db.models.base import Base

    
class Calendar(Base):
    __tablename__ = 'calendar'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    listing_id = Column(String(36), ForeignKey('listings.id'), nullable=False)
    date =  Column(DateTime)
    available =  Column(String)
    price_USD =  Column(Float)
    minimum_nights = Column(Float)
    maximum_nights = Column(Float)
    
    listing = relationship('Listing', back_populates='calendar')