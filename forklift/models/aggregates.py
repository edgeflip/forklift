from sqlalchemy import Column, Integer, Table
from sqlalchemy.ext.declarative import declared_attr
from forklift.settings import IP_SLUG, FBID_SLUG, VISIT_SLUG, FRIEND_SLUG, MISC_SLUG
from forklift.warehouse.definition import IpFactsHourly, VisitFactsHourly, FriendFbidFactsHourly, FbidFactsHourly, MiscFactsHourly
from .base import Base


class HourlyAggregateMixin(object):
    slug = None

    @classmethod
    def sqlalchemy_columns(cls):
        dims = [Column(dimension.column_name(), dimension.datatype, primary_key=True) for dimension in cls.table.dimensions()]
        facts = [Column(fact.slug, Integer) for fact in cls.table.facts()]
        return dims + facts


    @declared_attr
    def __table__(cls):
        columns = cls.sqlalchemy_columns()
        return Table(
            cls.table.tablename(),
            Base.metadata,
            *columns,
            extend_existing=True
        )
            

class FbidFactHourly(HourlyAggregateMixin, Base):
    slug = FBID_SLUG
    table = FbidFactsHourly


class FriendFbidFactHourly(HourlyAggregateMixin, Base):
    slug = FRIEND_SLUG
    table = FriendFbidFactsHourly


class VisitFactHourly(HourlyAggregateMixin, Base):
    slug = VISIT_SLUG
    table = VisitFactsHourly


class MiscFactHourly(HourlyAggregateMixin, Base):
    slug = MISC_SLUG
    table = MiscFactsHourly


class IpFactHourly(HourlyAggregateMixin, Base):
    slug = IP_SLUG
    table = IpFactsHourly
