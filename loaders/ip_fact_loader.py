from .fact_loader import HourlyFactLoader
from warehouse.definition import IpFactsHourly

class IpFactLoader(HourlyFactLoader):
    joins = [
        'join visits using (visit_id)',
    ]
    aggregate_table = IpFactsHourly
    dimension_source = 'visits'
