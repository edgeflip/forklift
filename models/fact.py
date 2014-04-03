from base import Base
from sqlalchemy import Column, Integer, String

class Fact(Base):
    __tablename__ = 'facts'

    fact_id = Column(Integer, primary_key=True)
    slug = Column(String)
    pretty_name = Column(String)
    expression = Column(String)
    dimension_id = Column(Integer)
