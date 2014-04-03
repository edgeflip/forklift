from base import Base
from sqlalchemy import Column, Integer, String, BigInteger, DateTime


class Visitor(Base):
    __tablename__ = 'visitors'

    visitor_id = Column(Integer, primary_key=True)
    uuid = Column(String)
    fbid = Column(BigInteger)
    updated = Column(DateTime)
    created = Column(DateTime)


class Visit(Base):
    __tablename__ = 'visits'

    visit_id = Column(Integer, primary_key=True)
    visitor_id = Column(Integer)
    session_id = Column(String)
    app_id = Column('appid', BigInteger)
    ip = Column(String)
    user_agent = Column(String)
    referer = Column(String)
    source = Column(String)
    updated = Column(DateTime)
    created = Column(DateTime)


class Event(Base):
    __tablename__ = 'events'

    event_id = Column(Integer, primary_key=True)
    visit_id = Column(Integer)
    event_type = Column('type', String)
    campaign_id = Column(Integer)
    client_content = Column(Integer)
    content = Column(String)
    friend_fbid = Column(BigInteger)
    activity_id = Column(BigInteger)
    event_datetime = Column(DateTime)
    updated = Column(DateTime)
    created = Column(DateTime)

