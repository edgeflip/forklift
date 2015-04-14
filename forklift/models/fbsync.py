from .base import Base
from sqlalchemy import Column, String, BigInteger, DateTime, Boolean


class ForkliftMixin(object):
    __table_args__ = {'schema': 'forklift'}


class FBSyncPageTask(Base, ForkliftMixin):
    __tablename__ = 'fbsync_page_tasks'

    efid = Column(BigInteger, primary_key=True)
    crawl_type = Column(String, primary_key=True)
    url = Column(String, primary_key=True)
    post_id = Column(String)
    post_from = Column(BigInteger)
    extracted = Column(DateTime)
    transformed = Column(DateTime)
    loaded = Column(DateTime)
    last_run_id = Column(String)
    key_name = Column(String)


class FBSyncRunList(Base, ForkliftMixin):
    __tablename__ = 'fbsync_run_lists'

    run_id = Column(String, primary_key=True)
    efid = Column(BigInteger, primary_key=True)


class FBSyncRun(Base, ForkliftMixin):
    __tablename__ = 'fbsync_runs'

    run_id = Column(String, primary_key=True)
    load_started = Column(Boolean)
