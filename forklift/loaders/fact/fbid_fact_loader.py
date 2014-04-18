from .fact_loader import HourlyFactLoader
from forklift.warehouse.definition import FbidFactsHourly

class FbidFactLoader(HourlyFactLoader):
    aggregate_table = FbidFactsHourly
    joins = [
        'join visits using (visit_id)',
        'join visitors using (visitor_id)',
    ]
    dimension_source = 'visitors'
