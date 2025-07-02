import uuid
from sqlalchemy import Column, Integer, String, Float, ARRAY, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from db.models.base import Base


class Reviews(Base):
    __tablename__ = 'airbnb_reviews'
    __table_args__ = {'schema': 'silver'}
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    listing_id = Column(String(36), ForeignKey('silver.airbnb_listings.id'), nullable=False)
    date = Column(DateTime)
    reviewer_id = Column(String)
    reviewer_name = Column(String)
    comments = Column(String)
    
    listing = relationship('Listing', back_populates='silver.airbnb_reviews')