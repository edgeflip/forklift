from .base import Base
from sqlalchemy import Column, String, BigInteger, DateTime, Numeric, ForeignKey
from sqlalchemy.orm import relationship


class MagnusMixin(object):
    __table_args__ = {'schema': 'magnus'}


class FBUserToken(Base, MagnusMixin):
    __tablename__ = 'fb_user_tokens'

    user_token_id = Column(BigInteger, primary_key=True)
    app_user_id = Column(BigInteger)
    access_token = Column(String)
    expiration = Column(DateTime)
    api = Column(Numeric)
    created = Column(DateTime)
    updated = Column(DateTime)


class EFUser(Base, MagnusMixin):
    __tablename__ = 'ef_users'

    efid = Column(BigInteger, primary_key=True)
    name = Column(String)
    email = Column(String)
    created = Column(DateTime)
    updated = Column(DateTime)


class FBAppUser(Base, MagnusMixin):
    __tablename__ = 'fb_app_users'

    app_user_id = Column(BigInteger, primary_key=True)
    fb_app_id = Column(BigInteger)
    efid = Column(BigInteger, ForeignKey(EFUser.efid))
    fbid = Column(BigInteger)
    created = Column(DateTime)
    updated = Column(DateTime)

    user = relationship(EFUser)
