import uuid
from sqlalchemy import Column, Integer, String, Float, ARRAY, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from db.models.base import Base

class Host(Base):
    __tablename__ = 'hosts'
    
    id = Column(String, primary_key=True)
    host_url = Column(String, nullable=True)
    host_name = Column(String, nullable=False)
    host_since =  Column(DateTime)
    host_location = Column(String)
    host_about =  Column(String)
    host_response_time = Column(String)
    host_response_rate = Column(String)
    host_acceptance_rate = Column(String)
    host_is_superhost = Column(String)
    host_thumbnail_url = Column(String)
    host_picture_url = Column(String)
    host_neighbourhood = Column(String)
    host_listings_count = Column(String)
    host_total_listings_count = Column(String)
    host_verifications = Column(ARRAY(String))
    host_has_profile_pic = Column(String)
    host_identity_verified = Column(String)