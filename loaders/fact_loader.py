from db.utils import staging_table

class HourlyFactLoader(object):

    def columns(self):
        return ['hour'] + [column.column_name() for column in self.aggregate_table.columns() if column.column_name() != 'hour']


    def destination_table(self):
        return self.aggregate_table.tablename()


    def dimensions(self):
        return [dimension.column_name() for dimension in self.aggregate_table.dimensions() if dimension.column_name() != 'hour']


    def where_expressions(self, hour):
        return ["events.created between '{hour}' and timestamp '{hour}' + interval '1 hour'".format(hour=hour)]


    def load_hour(self, hour, connection):
        with staging_table(self.destination_table(), connection) as staging_table_name:
            self.stage_hour(hour, staging_table_name, connection)
            self.upsert(hour, staging_table_name, self.destination_table(), connection)


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
            facts=",\n".join([fact.expression for fact in self.aggregate_table.facts()]),
            joins="\n".join(self.joins),
            staging_table=staging_table_name,
            where_clause="and \n".join(self.where_expressions(formatted_hour)),
            hour=formatted_hour,
        )
        connection.execute(sql)


    def upsert(self, hour, staging_table_name, target_table_name, connection):
        with connection.begin_nested():
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
