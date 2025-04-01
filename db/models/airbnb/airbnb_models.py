from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class Host(Base):
    __tablename__ = 'hosts'
    
    id = Column(Integer, primary_key=True)
    host_url = Column(String, nullable=True)
    host_name = Column()
    host_since =  
    host_location = 
    host_about =  
    host_response_time =  
    host_response_rate_% = 
    host_acceptance_rate_% =  
    host_is_superhost =  
    host_thumbnail_url = 
    host_picture_url =  
    host_neighbourhood =  
    host_listings_count = 
    host_total_listings_count =  
    host_verifications = 
    host_has_profile_pic =  
    host_identity_verified = 