from .fact_loader import HourlyFactLoader
from warehouse.definition import MiscFactsHourly

class MiscFactLoader(HourlyFactLoader):
    joins = []
    aggregate_table = MiscFactsHourly
    dimension_source = None
