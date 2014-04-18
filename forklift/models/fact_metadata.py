from .base import Base
from sqlalchemy import Column, Integer, String

class FactMetadata(Base):
    __tablename__ = 'fact_metadata'

    fact_id = Column(Integer, primary_key=True)
    slug = Column(String)
    pretty_name = Column(String)
    expression = Column(String)
    dimension_id = Column(Integer)
