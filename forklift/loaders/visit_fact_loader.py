from .fact_loader import HourlyFactLoader
from forklift.warehouse.definition import VisitFactsHourly

class VisitFactLoader(HourlyFactLoader):
    joins = []
    aggregate_table = VisitFactsHourly
    dimension_source = 'events'
