from settings import SQLALCHEMY_URL
from sqlalchemy import create_engine

engine = create_engine(SQLALCHEMY_URL)
