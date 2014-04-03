from sqlalchemy import BigInteger, Column, DateTime, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from core.slugs import IP_SLUG, FBID_SLUG, VISIT_SLUG, FRIEND_SLUG, MISC_SLUG
from warehouse.definition import IpFactsHourly, VisitFactsHourly, FriendFbidFactsHourly, FbidFactsHourly, MiscFactsHourly

Base = declarative_base()

class HourlyAggregateMixin(object):
    slug = None

    @classmethod
    def sqlalchemy_columns(cls):
        dims = [Column(dimension.column_name(), dimension.datatype, primary_key=True) for dimension in cls.table.dimensions()]
        facts = [Column(fact.slug, Integer) for fact in cls.table.facts]
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


class Dimension(Base):
    __tablename__ = 'dimensions'

    dimension_id = Column(Integer, primary_key=True)
    slug = Column(String)
    pretty_name = Column(String)
    column = Column(String)
    source_table = Column(String)


class Fact(Base):
    __tablename__ = 'facts'

    fact_id = Column(Integer, primary_key=True)
    slug = Column(String)
    pretty_name = Column(String)
    expression = Column(String)
    dimension_id = Column(Integer)

