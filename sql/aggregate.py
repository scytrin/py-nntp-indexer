#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.

from sql import Column

__all__ = ['Avg', 'BitAnd', 'BitOr', 'BoolAnd', 'BoolOr', 'Count', 'Every',
    'Max', 'Min', 'Stddev', 'Sum', 'Variance']


class Aggregate(Column):
    __slots__ = ()
    _sql = ''

    def __init__(self, column):
        super(Aggregate, self).__init__(column.table, column.name)

    def __str__(self):
        return '%s(%s)' % (self._sql, super(Aggregate, self).__str__())


class Avg(Aggregate):
    __slots__ = ()
    _sql = 'AVG'


class BitAnd(Aggregate):
    __slots__ = ()
    _sql = 'BIT_AND'


class BitOr(Aggregate):
    __slots__ = ()
    _sql = 'BIT_OR'


class BoolAnd(Aggregate):
    __slots__ = ()
    _sql = 'BOOL_AND'


class BoolOr(Aggregate):
    __slots__ = ()
    _sql = 'BOOL_OR'


class Count(Aggregate):
    __slots__ = ()
    _sql = 'COUNT'


class Every(Aggregate):
    __slots__ = ()
    _sql = 'EVERY'


class Max(Aggregate):
    __slots__ = ()
    _sql = 'MAX'


class Min(Aggregate):
    __slots__ = ()
    _sql = 'MIN'


class Stddev(Aggregate):
    __slots__ = ()
    _sql = 'Stddev'


class Sum(Aggregate):
    __slots__ = ()
    _sql = 'SUM'


class Variance(Aggregate):
    __slots__ = ()
    _sql = 'VARIANCE'
