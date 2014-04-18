from sqlalchemy import Integer


class Column(object):
    def __init__(self, *args, **kwargs):
        self.slug = kwargs['slug']
        self.pretty_name = kwargs['pretty_name']
        self.datatype = kwargs['datatype']


class Fact(Column):
    def __init__(self, *args, **kwargs):
        super(Fact, self).__init__(datatype=Integer, *args, **kwargs)
        self.expression = kwargs['expression']

    @property
    def column_name(self):
        return self.slug


class Dimension(Column):
    def __init__(self, *args, **kwargs):
        super(Dimension, self).__init__(*args, **kwargs)
        self._column_name = kwargs['column_name']
        if 'source_table' in kwargs:
            self.source_table = kwargs['source_table']

    @property
    def column_name(self):
        return self._column_name

