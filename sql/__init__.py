#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.

__version__ = '0.1'
__all__ = ['Flavor', 'Table', 'Column', 'Join', 'Asc', 'Desc']

import string
from threading import local, currentThread


def alias(i, letters=string.ascii_lowercase):
    '''
    Generate a unique alias based on integer

    >>> [alias(n) for n in range(6)]
    ['a', 'b', 'c', 'd', 'e', 'f']
    >>> [alias(n) for n in range(26, 30)]
    ['ba', 'bb', 'bc', 'bd']
    >>> [alias(26**n) for n in range(5)]
    ['b', 'ba', 'baa', 'baaa', 'baaaa']
    '''
    s = ''
    length = len(letters)
    while True:
        r = i % length
        s = letters[r] + s
        i /= length
        if i == 0:
            break
    return s


class Flavor(object):
    '''
    Contains the flavor of SQL

    Contains:
        max_limit - limit to use if there is no limit but an offset
        paramstyle - state the type of parameter marker formatting
        ilike - support ilike extension
        function_mapping - dictionary with Function to replace
    '''

    def __init__(self, max_limit=None, paramstyle='format', ilike=False,
            function_mapping=None):
        self.max_limit = max_limit
        self.paramstyle = paramstyle
        self.ilike = ilike
        self.function_mapping = function_mapping or {}

    @property
    def param(self):
        if self.paramstyle == 'format':
            return '%s'
        elif self.paramstyle == 'qmark':
            return '?'

    @staticmethod
    def set(flavor):
        '''Set this thread's flavor to flavor.'''
        currentThread().__sql_flavor__ = flavor

    @staticmethod
    def get():
        '''
        Return this thread's flavor.

        If this thread does not yet have a flavor, returns a new flavor and
        sets this thread's flavor.
        '''
        try:
            return currentThread().__sql_flavor__
        except AttributeError:
            flavor = Flavor()
            currentThread().__sql_flavor__ = flavor
            return flavor


class AliasManager(object):
    '''
    Context Manager for unique alias generation
    '''
    __slots__ = 'local'

    local = local()
    local.alias = None
    local.nested = 0

    @classmethod
    def __enter__(cls):
        if cls.local.alias is None:
            cls.local.alias = {}
        cls.local.nested += 1

    @classmethod
    def __exit__(cls, type, value, traceback):
        cls.local.nested -= 1
        if not cls.local.nested:
            cls.local.alias = None

    @classmethod
    def get(cls, from_):
        if cls.local.alias is None:
            return ''
        i = len(cls.local.alias)
        cls.local.alias.setdefault(from_, alias(i))
        return cls.local.alias[from_]


class Query(object):
    __slots__ = ()

    @property
    def params(self):
        return ()

    def __iter__(self):
        yield str(self)
        yield self.params


class FromItem(object):
    __slots__ = ()

    @property
    def alias(self):
        return AliasManager.get(self)

    def __getattr__(self, name):
        return Column(self, name)

    def __add__(self, other):
        assert isinstance(other, FromItem)
        return From((self, other))

    def select(self, *args, **kwargs):
        return From((self,)).select(*args, **kwargs)


class Select(Query, FromItem):
    __slots__ = ('__columns', '__where', '__group_by', '__having',
        '__order_by', '__limit', '__offset', '__for_', 'from_')

    def __init__(self, columns, from_=None, where=None, group_by=None,
            having=None, order_by=None, limit=None, offset=None,
            for_=None):
        super(Select, self).__init__()
        self.__columns = None
        self.__where = None
        self.__group_by = None
        self.__having = None
        self.__order_by = None
        self.__limit = None
        self.__offset = None
        self.__for_ = None
        # TODO ALL|DISTINCT
        self.columns = columns
        self.from_ = from_
        self.where = where
        self.group_by = group_by
        self.having = having
        self.order_by = order_by
        self.limit = limit
        self.offset = offset
        self.for_ = for_

    @property
    def columns(self):
        return self.__columns

    @columns.setter
    def columns(self, value):
        assert all(isinstance(col, Column) for col in value)
        self.__columns = tuple(value)

    @property
    def where(self):
        return self.__where

    @where.setter
    def where(self, value):
        from sql.operators import And, Or
        if value is not None:
            assert isinstance(value, (Column, And, Or))
        self.__where = value

    @property
    def group_by(self):
        return self.__group_by

    @group_by.setter
    def group_by(self, value):
        if value is not None:
            if isinstance(value, Column):
                value = [value]
            assert all(isinstance(col, Column) for col in value)
        self.__group_by = value

    @property
    def having(self):
        return self.__having

    @having.setter
    def having(self, value):
        from sql.operators import And, Or
        if value is not None:
            assert isinstance(value, (Column, And, Or))
        self.__having = value

    @property
    def order_by(self):
        return self.__order_by

    @order_by.setter
    def order_by(self, value):
        if value is not None:
            if isinstance(value, Column):
                value = [value]
            assert all(isinstance(col, Column) for col in value)
        self.__order_by = value

    @property
    def limit(self):
        return self.__limit

    @limit.setter
    def limit(self, value):
        if value is not None:
            assert isinstance(value, (int, long))
        self.__limit = value

    @property
    def offset(self):
        return self.__offset

    @offset.setter
    def offset(self, value):
        if value is not None:
            assert isinstance(value, (int, long))
        self.__offset = value

    @property
    def for_(self):
        return self.__for_

    @for_.setter
    def for_(self, value):
        if value is not None:
            if isinstance(value, For):
                value = [value]
            assert isinstance(all(isinstance(f, For) for f in value))
        self.__for_ = value

    def __str__(self):
        with AliasManager():
            from_ = str(self.from_)
            columns = ', '.join(map(str, self.columns))
            where = ''
            if self.where:
                where = ' WHERE ' + str(self.where)
            group_by = ''
            if self.group_by:
                group_by = ' GROUP BY ' + ', '.join(map(str, self.group_by))
            having = ''
            if self.having:
                having = ' HAVING ' + str(self.having)
            order_by = ''
            if self.order_by:
                order_by = ' ORDER BY ' + ', '.join(map(str, self.order_by))
            limit = ''
            if self.limit is not None:
                limit = ' LIMIT %s' % self.limit
            elif self.offset is not None:
                max_limit = Flavor.get().max_limit
                if max_limit:
                    limit = ' LIMIT %s' % max_limit
            offset = ''
            if self.offset:
                offset = ' OFFSET %s' % self.offset
            for_ = ''
            if self.for_ is not None:
                for_ = ' '.join(self.for_)
            return ('SELECT %s FROM %s' % (columns, from_) + where + group_by
                + having + order_by + limit + offset + for_)

    @property
    def params(self):
        p = ()
        for column in self.columns:
            p += column.params
        p += self.from_.params
        if self.where:
            p += self.where.params
        return p

    def __or__(self, other):
        return Union(self, other)

    def __and__(self, other):
        return Interesect(self, other)

    def __sub__(self, other):
        return Except(self, other)


class Insert(Query):
    __slots__ = ('__table', '__columns', '__values', '__returning')

    def __init__(self, table, columns=None, values=None, returning=None):
        self.__table = None
        self.__columns = None
        self.__values = None
        self.__returning = None
        self.table = table
        self.columns = columns
        self.values = values
        self.returning = returning

    @property
    def table(self):
        return self.__table

    @table.setter
    def table(self, value):
        assert isinstance(value, Table)
        self.__table = value

    @property
    def columns(self):
        return self.__columns

    @columns.setter
    def columns(self, value):
        if value is not None:
            assert all(isinstance(col, Column) for col in value)
            assert all(col.table == self.table for col in value)
        self.__columns = value

    @property
    def values(self):
        return self.__values

    @values.setter
    def values(self, value):
        if value is not None:
            assert isinstance(value, (list, Select))
            if isinstance(value, list):
                if not all(isinstance(x, list) for x in value):
                    value = [value]
        self.__values = value

    @property
    def returning(self):
        return self.__returning

    @returning.setter
    def returning(self, value):
        if value is not None:
            assert isinstance(value, list)
        self.__returning = value

    def __str__(self):
        columns = ''
        if self.columns:
            assert all(col.table == self.table for col in self.columns)
            columns = ' (' + ', '.join(map(str, self.columns)) + ')'
        param = Flavor.get().param
        if isinstance(self.values, list):
            values = ' VALUES ' + ', '.join(
                '(' + ', '.join((param,) * len(value)) + ')'
                for value in self.values)
            # TODO manage DEFAULT
        elif isinstance(self.values, Select):
            values = ' VALUES (%s)' % str(self.values)
        elif self.values is None:
            values = ' DEFAULT VALUES'
        returning = ''
        if self.returning:
            returning = ' RETURNING ' + ', '.join(map(str, self.returning))
        return 'INSERT INTO %s' % self.table + columns + values + returning

    @property
    def params(self):
        if isinstance(self.values, list):
            return sum((tuple(value) for value in self.values), ())
        elif isinstance(self.values, Select):
            return list(self.values)[1]
        else:
            return ()


class Update(Insert):
    __slots__ = ('__where', '__values', 'from_')

    def __init__(self, table, columns, values, from_=None, where=None):
        super(Update, self).__init__(table, columns=columns, values=values)
        self.__where = None
        self.from_ = From(from_) if from_ else None
        self.where = where

    @property
    def values(self):
        return self.__values

    @values.setter
    def values(self, value):
        if isinstance(value, Select):
            value = [value]
        assert isinstance(value, list)
        self.__values = value

    @property
    def where(self):
        return self.__where

    @where.setter
    def where(self, value):
        from sql.operators import And, Or
        if value is not None:
            assert isinstance(value, (Column, And, Or))
        self.__where = value

    @staticmethod
    def _format(value):
        if isinstance(value, Column):
            return str(value)
        elif isinstance(value, Select):
            return '(%s)' % value
        else:
            return Flavor.get().param

    def __str__(self):
        assert all(col.table == self.table for col in self.columns)
        # Get columns without alias
        columns = ' (' + ', '.join(map(str, self.columns)) + ')'
        with AliasManager():
            from_ = ''
            if self.from_:
                from_ = ' FROM %s' % str(self.from_)
            values = ' = (' + ', '.join(map(self._format, self.values)) + ')'
            where = ''
            if self.where:
                where = ' WHERE ' + str(self.where)
            return ('UPDATE %s SET' % self.table + columns + values + from_
                + where)

    @property
    def params(self):
        p = ()
        for value in self.values:
            if isinstance(value, Column):
                p += value.params
            elif isinstance(value, Select):
                p += list(value)[1]
            else:
                p += (value,)
        if self.from_:
            p += self.from_.params
        if self.where:
            p += self.where.params
        return p


class Delete(Query):
    __slots__ = ('__table', '__where', '__returning', 'only')

    def __init__(self, table, only=False, using=None, where=None,
            returning=None):
        self.__table = None
        self.__where = None
        self.__returning = None
        self.table = table
        self.only = only
        # TODO using (not standard)
        self.where = where
        self.returning = returning

    @property
    def table(self):
        return self.__table

    @table.setter
    def table(self, value):
        assert isinstance(value, Table)
        self.__table = value

    @property
    def where(self):
        return self.__where

    @where.setter
    def where(self, value):
        from sql.operators import And, Or
        if value is not None:
            assert isinstance(value, (Column, And, Or))
        self.__where = value

    @property
    def returning(self):
        return self.__returning

    @returning.setter
    def returning(self, value):
        if value is not None:
            assert isinstance(value, list)
        self.__returning = value

    def __str__(self):
        only = ' ONLY' if self.only else ''
        where = ''
        if self.where:
            where = ' WHERE ' + str(self.where)
        returning = ''
        if self.returning:
            returning = ' RETURNING ' + ', '.join(map(str, self.returning))
        return 'DELETE FROM%s %s' % (only, self.table) + where + returning

    @property
    def params(self):
        return self.where.params if self.where else ()


class CombiningQuery(Query, FromItem):
    __slots__ = ('query1', 'query2', 'all_')
    _operator = ''

    def __init__(self, query1, query2, all_=False):
        super(CombiningQuery, self).__init__()
        assert isinstance(query1, Select) and isinstance(query2, Select)
        self.query1 = query1
        self.query2 = query2
        self.all_ = all_

    def __str__(self):
        with AliasManager():
            return '%s %s %s%s' % (self.query1, self._operator,
                'ALL ' if self.all_ else '', self.query2)

    @property
    def params(self):
        return self.query1.params + self.query2.params


class Union(CombiningQuery):
    __slots__ = ()
    _operator = 'UNION'


class Interesect(CombiningQuery):
    __slots__ = ()
    _operator = 'INTERSECT'


class Except(CombiningQuery):
    __slots__ = ()
    _operator = 'EXCEPT'


class Table(FromItem):
    __slots__ = '__name'

    def __init__(self, name):
        super(Table, self).__init__()
        self.__name = name

    def __str__(self):
        if self.alias:
            return '"%s" AS "%s"' % (self.__name, self.alias)
        else:
            return '"%s"' % self.__name

    @property
    def params(self):
        return ()

    def insert(self, columns=None, values=None, returning=None):
        return Insert(self, columns=columns, values=values,
            returning=returning)

    def update(self, columns, values, from_=None, where=None):
        return Update(self, columns=columns, values=values, from_=from_,
            where=where)

    def delete(self, only=False, using=None, where=None, returning=None):
        return Delete(self, only=only, using=using, where=where,
            returning=returning)


class Join(FromItem):
    __slots__ = ('__left', '__right', '__condition', '__type_')

    def __init__(self, left, right, type_='INNER', condition=None):
        super(Join, self).__init__()
        self.__left, self.__right = None, None
        self.__condition = None
        self.__type_ = None
        self.left = left
        self.right = right
        self.condition = condition
        self.type_ = type_

    @property
    def left(self):
        return self.__left

    @left.setter
    def left(self, value):
        assert isinstance(value, FromItem)
        self.__left = value

    @property
    def right(self):
        return self.__right

    @right.setter
    def right(self, value):
        assert isinstance(value, FromItem)
        self.__right = value

    @property
    def condition(self):
        return self.__condition

    @condition.setter
    def condition(self, value):
        from sql.operators import And, Or
        if value is not None:
            assert isinstance(value, (Column, And, Or))
        self.__condition = value

    @property
    def type_(self):
        return self.__type_

    @type_.setter
    def type_(self, value):
        value = value.upper()
        assert value in ('INNER', 'LEFT', 'LEFT OUTER',
            'RIGHT', 'RIGHT OUTER', 'FULL', 'FULL OUTER', 'CROSS')
        self.__type_ = value

    def __str__(self):
        def format(from_):
            if isinstance(from_, Select):
                return '(%s)' % from_
            return str(from_)
        join = '%s %s JOIN %s' % (format(self.left), self.type_,
            format(self.right))
        if self.condition:
            condition = ' ON %s' % self.condition
        else:
            condition = ''
        return join + condition

    @property
    def params(self):
        p = ()
        for item in (self.left, self.right):
            if hasattr(item, 'params'):
                p += item.params
        if hasattr(self.condition, 'params'):
            p += self.condition.params
        return p

    def __getattr__(self, name):
        raise AttributeError

# TODO function as FromItem


class From(list):
    __slots__ = ()

    def select(self, *args, **kwargs):
        if not args:
            args = tuple(Column(x, '*') for x in self)
        return Select(args, from_=self, **kwargs)

    def __str__(self):
        def format(from_):
            if isinstance(from_, Query):
                return '(%s) AS "%s"' % (from_, from_.alias)
            return str(from_)
        return ', '.join(map(format, self))

    @property
    def params(self):
        return sum((from_.params for from_ in self), ())

    def __add__(self, other):
        assert isinstance(other, (Join, Select))
        return From(super(From, self).__add__([other]))


class Column(object):
    __slots__ = ('__from', '__name')

    def __init__(self, from_, name):
        self.__from = from_
        self.__name = name

    @property
    def table(self):
        return self.__from

    @property
    def name(self):
        return self.__name

    def __str__(self):
        if self.__name == '*':
            t = '%s'
        else:
            t = '"%s"'
        if self.__from.alias:
            t = '"%s".' + t
            return t % (self.__from.alias, self.__name)
        else:
            return t % self.__name

    @property
    def params(self):
        return ()

    def __and__(self, other):
        from sql.operators import And
        return And((self, other))

    def __or__(self, other):
        from sql.operators import Or
        return Or((self, other))

    def __invert__(self):
        from sql.operators import Not
        return Not(self)

    def __add__(self, other):
        from sql.operators import Add
        return Add(self, other)

    def __sub__(self, other):
        from sql.operators import Sub
        return Sub(self, other)

    def __mul__(self, other):
        from sql.operators import Mul
        return Mul(self, other)

    def __floordiv__(self, other):
        from sql.operators import FloorDiv
        return FloorDiv(self, other)

    def __mod__(self, other):
        from sql.operators import Mod
        return Mod(self, other)

    def __pow__(self, other):
        from sql.operators import Pow
        return Pow(self, other)

    def __neg__(self):
        from sql.operators import Neg
        return Neg(self)

    def __pos__(self):
        from sql.operators import Pos
        return Pos(self)

    def __abs__(self):
        from sql.operators import Abs
        return Abs(self)

    def __lshift__(self, other):
        from sql.operators import LShift
        return LShift(self, other)

    def __rshift__(self, other):
        from sql.operators import RShift
        return RShift(self, other)

    def __lt__(self, other):
        from sql.operators import Less
        return Less(self, other)

    def __le__(self, other):
        from sql.operators import LessEqual
        return LessEqual(self, other)

    def __eq__(self, other):
        from sql.operators import Equal
        return Equal(self, other)

    def __ne__(self, other):
        from sql.operators import NotEqual
        return NotEqual(self, other)

    def __gt__(self, other):
        from sql.operators import Greater
        return Greater(self, other)

    def __ge__(self, other):
        from sql.operators import GreaterEqual
        return GreaterEqual(self, other)

    def in_(self, values):
        from sql.operators import In
        return In(self, values)

    def like(self, test):
        from sql.operators import Like
        return Like(self, test)

    def ilike(self, test):
        from sql.operators import ILike
        return ILike(self, test)


class Order(Column):
    __slots__ = ()
    _sql = ''

    def __init__(self, column):
        super(Order, self).__init__(column.table, column.name)
        # TODO USING

    def __str__(self):
        return '%s %s' % (super(Order, self).__str__(), self._sql)


class Asc(Order):
    __slots__ = ()
    _sql = 'ASC'


class Desc(Order):
    __slots__ = ()
    _sql = 'DESC'


class For(object):
    __slots__ = ('__tables', '__type_', 'nowait')

    def __init__(self, type_, *tables, **kwargs):
        self.__tables = None
        self.__type_ = None
        self.tables = list(tables)
        self.type_ = type_
        self.nowait = kwargs.get('nowait')

    @property
    def tables(self):
        return self.__tables

    @tables.setter
    def tables(self, value):
        if not isinstance(value, list):
            value = list(value)
        all(isinstance(table, Table) for table in value)
        self.__tables = value

    @property
    def type_(self):
        return self.__type_

    @type_.setter
    def type_(self, value):
        value = value.upper()
        assert value in ('UPDATE', 'SHARE')
        self.__type_ = value

    def __str__(self):
        tables = ''
        if self.tables:
            tables = ' OF ' + ', '.join(map(str, self.tables))
        nowait = ''
        if self.nowait:
            nowait = ' NOWAIT'
        return ('FOR %s' % self.type_) + tables + nowait
