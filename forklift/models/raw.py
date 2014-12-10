from .base import Base
from sqlalchemy import Boolean, Column, Integer, String, BigInteger, DateTime


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


class Client(Base):
    __tablename__ = 'clients'

    client_id = Column(Integer, primary_key=True)
    name = Column(String)
    fb_app_name = Column(String)
    fb_app_id = Column(String)
    domain = Column(String)
    subdomain = Column(String)
    create_dt = Column(DateTime)
    source_parameter = Column(String)
    codename = Column(String)


class Campaign(Base):
    __tablename__ = 'campaigns'

    campaign_id = Column(Integer, primary_key=True)
    client_id = Column(Integer)
    name = Column(String)
    description = Column(String)
    is_deleted = Column(Boolean)
    create_dt = Column(DateTime)
    delete_dt = Column(DateTime)


class CampaignProperty(Base):
    __tablename__ = 'campaign_properties'

    campaign_property_id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer)
    client_faces_url = Column(String)
    client_thanks_url = Column(String)
    client_error_url = Column(String)
    fallback_campaign_id = Column(Integer)
    fallback_content_id = Column(Integer)
    fallback_is_cascading = Column(Boolean)
    min_friends = Column(Integer)
    start_dt = Column(DateTime)
    end_dt = Column(DateTime)
    root_campaign_id = Column(Integer)
    num_faces = Column(Integer)
    status = Column(String)
    content_id = Column(Integer)


class EdgeflipFbid(Base):
    __tablename__ = 'edgeflip_fbids'

    fbid = Column(BigInteger, primary_key=True)
