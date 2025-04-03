import uuid
from sqlalchemy import Column, Integer, String, Float, ARRAY, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from db.models.base import Base

class Neighbourhood(Base):
    __tablename__ = 'neighbourhood'
    id = Column(String, primary_key=True, nullable=False, default=lambda: str(uuid.uuid4()))
    neighbourhood = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)