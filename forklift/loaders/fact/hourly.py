from abc import abstractproperty, ABCMeta
from textwrap import dedent
import time

from forklift.db.utils import staging_table
from forklift.warehouse.definition import FbidFactsHourly, FriendFbidFactsHourly, IpFactsHourly, MiscFactsHourly, VisitFactsHourly

class HourlyFactLoader(object):
    __metaclass__ = ABCMeta
    joins = ()

    @abstractproperty
    def aggregate_table(self):
        return None

    def columns(self):
        return ['hour'] + [column.column_name for column in self.aggregate_table.columns() if column.column_name != 'hour']


    @property
    def destination_table(self):
        return self.aggregate_table.tablename()


    def dimensions(self):
        return [dimension.column_name for dimension in self.aggregate_table.dimensions() if dimension.column_name != 'hour']


    def where_expressions(self, hour):
        return (
            "events.created between '{hour}' and timestamp '{hour}' + interval '1 hour'".format(hour=hour),
            "campaign_id is not null",
        ) + tuple("{}.{} is not null".format(dim.source_table, dim.column_name) for dim in self.aggregate_table.extra_dimensions)


    def load_hour(self, hour, connection, logger):
        start_time = time.time()
        with staging_table(self.destination_table, connection) as staging_table_name:
            self.stage_hour(hour, staging_table_name, connection)
            num_rows = connection.execute('select count(*) as num_rows from {}'.format(staging_table_name)).fetchone()[0]
            self.upsert(hour, staging_table_name, self.destination_table, connection)
            end_time = time.time()
            logger.info('Completed load of {} rows for hour {} in {:.2f}s'.format(
                int(num_rows),
                hour.strftime("%Y-%m-%d %H"),
                end_time - start_time
            ))


    def stage_hour(self, hour, staging_table_name, connection):
        formatted_hour = hour.strftime("%Y-%m-%d %H:%M:%S")
        sql = """
            insert into {staging_table}
            ({columns})
            select
            '{hour}',
            {dimensions},
            {facts}
            from events
            {joins}
            where
            {where_clause}
            group by
            {dimensions}
        """.format(
            columns=",".join(self.columns()),
            dimensions=",\n".join(self.dimensions()),
            facts=",\n".join(fact.expression for fact in self.aggregate_table.facts),
            joins="\n".join(self.joins),
            staging_table=staging_table_name,
            where_clause=" and \n".join(self.where_expressions(formatted_hour)),
            hour=formatted_hour,
        )
        connection.execute(dedent(sql))


    def upsert(self, hour, staging_table_name, target_table_name, connection):
        with connection.begin():
            connection.execute("""
                delete from {target_table}
                where hour = '{hour}'
            """.format(
                target_table=target_table_name,
                hour=hour.strftime("%Y-%m-%d %H:%M:%S"),
            ))

            connection.execute("""
                insert into {target_table}
                select * from {staging_table}
            """.format(
                target_table=target_table_name,
                staging_table=staging_table_name,
            ))


class FbidHourlyFactLoader(HourlyFactLoader):
    aggregate_table = FbidFactsHourly
    joins = (
        'join visits using (visit_id)',
        'join visitors using (visitor_id)',
    )


class FriendFbidHourlyFactLoader(HourlyFactLoader):
    aggregate_table = FriendFbidFactsHourly


class IpHourlyFactLoader(HourlyFactLoader):
    aggregate_table = IpFactsHourly
    joins = (
        'join visits using (visit_id)',
    )


class MiscHourlyFactLoader(HourlyFactLoader):
    aggregate_table = MiscFactsHourly


class VisitHourlyFactLoader(HourlyFactLoader):
    aggregate_table = VisitFactsHourly
