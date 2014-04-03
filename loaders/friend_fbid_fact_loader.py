from .fact_loader import HourlyFactLoader
from warehouse.definition import FriendFbidFactsHourly

class FriendFbidFactLoader(HourlyFactLoader):
    joins = []
    aggregate_table = FriendFbidFactsHourly
    dimension_source = 'events'
    
    def where_expressions(self, hour):
        return super(FriendFbidFactLoader, self).where_expressions(hour) + ['friend_fbid is not null']
