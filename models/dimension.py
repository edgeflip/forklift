from base import Base
from sqlalchemy import Column, Integer, String


class Dimension(Base):
    __tablename__ = 'dimensions'

    dimension_id = Column(Integer, primary_key=True)
    slug = Column(String)
    pretty_name = Column(String)
    column = Column(String)
    source_table = Column(String)
