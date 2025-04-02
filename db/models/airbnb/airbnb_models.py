import uuid
from sqlalchemy import Column, Integer, String, Float, ARRAY, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from utils.config import BASE_DIR

load_dotenv(os.path.join(BASE_DIR, 'config', '.env.postgres'))
DATABASE_URL = os.getenv('POSTGRES_URL')

engine = create_engine(DATABASE_URL)
Base = declarative_base()


class Host(Base):
    __tablename__ = 'hosts'
    
    id = Column(Integer, primary_key=True)
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
    
class Listing(Base):
    __tablename__ = 'listings'
    
    id =  Column(String(36), primary_key=True, default= lambda: str(uuid.uuid4()))
    listing_url =  Column(String, nullable=False)
    name =  Column(String)
    description =  Column(String)
    neighborhood_overview = Column(String)
    picture_url =  Column(String)
    neighbourhood =  Column(String)
    latitude =  Column(Float)
    longitude = Column(Float)
    property_type =  Column(String)
    room_type =  Column(String)
    accommodates =  Column(Integer)
    bathrooms = Column(Float)
    bathrooms_text =  Column(String)
    bedrooms =  Column(Float)
    beds =  Column(Float)
    price_USD =  Column(Float)
    minimum_nights = Column(Integer)
    maximum_nights =  Column(Integer)
    minimum_nights_avg_ntm =  Column(Float)
    maximum_nights_avg_ntm = Column(Float)
    has_availability =  Column(String)
    availability_30 =  Column(Integer)
    availability_60 = Column(Integer)
    availability_90 =  Column(Integer)
    availability_365 =  Column(Integer)
    number_of_reviews = Column(Integer)
    first_review =  Column(DateTime)
    last_review =  Column(DateTime)
    review_scores_rating = Column(Integer)
    instant_bookable =  Column(String)
    calculated_host_listings_count = Column(Integer)
    calculated_host_listings_count_entire_homes = Column(Integer)
    calculated_host_listings_count_private_rooms = Column(Integer)
    calculated_host_listings_count_shared_rooms =  Column(Integer)
    reviews_per_month = Column(Float)
    host_id =  Column(Integer, ForeignKey('hosts.id'), nullable=False)
    price_BRL =  Column(Float)
    price_category =  Column(String)
    first_review_filled = Column(DateTime)
    never_reviewd = Column(Integer)
    
    host = relationship('Host', back_populates='listings')
    
class Calendar(Base):
    __tablename__ = 'calendar'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    listing_id = Column(String, ForeignKey('listings.id'), nullable=False)
    date =  Column(DateTime)
    available =  Column(String)
    price_USD =  Column(Float)
    minimum_nights = Column(Float)
    maximum_nights = Column(Float)
    
    listing = relationship('Listing', back_populates='calendar')
    
    
Base.metadata.create_all(engine)
    
    
    
