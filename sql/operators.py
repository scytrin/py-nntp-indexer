#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.

from sql import Column, Select, Flavor

__all__ = ['And', 'Or', 'Not', 'Less', 'Greater', 'LessEqual', 'GreaterEqual',
    'Equal', 'NotEqual', 'Add', 'Sub', 'Mul', 'FloorDiv', 'Mod', 'Pow',
    'SquareRoot', 'CubeRoot', 'Factorial', 'Abs', 'BAnd', 'BOr', 'BXor',
    'BNot', 'LShift', 'RShift', 'Concat', 'Like', 'NotLike', 'ILike',
    'NotILike', 'In', 'NotIn', 'Exists', 'Any', 'Some', 'All']


class Operator(object):
    __slots__ = ()

    @property
    def table(self):
        return ''

    @property
    def name(self):
        return ''

    @property
    def _operands(self):
        return ()

    @property
    def params(self):
        params = ()
        for operand in self._operands:
            if isinstance(operand, Operator):
                params += operand.params
            elif isinstance(operand, Column):
                pass
            elif isinstance(operand, Select):
                params += list(operand)[1]
            elif isinstance(operand, (list, tuple)):
                params += tuple(operand)
            elif operand is None:
                pass
            else:
                params += (operand,)
        return params

    @staticmethod
    def _format(operand):
        param = Flavor.get().param
        if isinstance(operand, Column):
            return str(operand)
        elif isinstance(operand, Select):
            return '(%s)' % operand
        elif isinstance(operand, (list, tuple)):
            return '(' + ', '.join((param,) * len(operand)) + ')'
        elif operand is None:
            return ''
        else:
            return param

    def __str__(self):
        raise NotImplemented

    def __and__(self, other):
        if isinstance(other, And):
            return And([self] + other)
        else:
            return And((self, other))

    def __or__(self, other):
        if isinstance(other, Or):
            return Or([self] + other)
        else:
            return Or((self, other))


class UnaryOperator(Operator, Column):
    __slots__ = 'operand'
    _operator = ''

    def __init__(self, operand):
        self.operand = operand

    @property
    def _operands(self):
        return (self.operand,)

    def __str__(self):
        return '(%s %s)' % (self._operator, self.operand)


class BinaryOperator(Operator, Column):
    __slots__ = ('left', 'right')
    _operator = ''

    def __init__(self, left, right):
        self.left = left
        self.right = right

    @property
    def _operands(self):
        return (self.left, self.right)

    def __str__(self):
        return '(%s %s %s)' % (self._format(self.left), self._operator,
            self._format(self.right))

    def __invert__(self):
        return _INVERT[self.__class__](self.left, self.right)


class NaryOperator(list, Operator):
    __slots__ = ()
    _operator = ''

    @property
    def _operands(self):
        return self

    def __str__(self):
        return '(' + (' %s ' % self._operator).join(map(str, self)) + ')'


class And(NaryOperator):
    __slots__ = ()
    _operator = 'AND'


class Or(NaryOperator):
    __slots__ = ()
    _operator = 'OR'


class Not(UnaryOperator):
    __slots__ = ()
    _operator = 'NOT'


class Neg(UnaryOperator):
    __slots__ = ()
    _operator = '-'


class Pos(UnaryOperator):
    __slots__ = ()
    _operator = '+'


class Less(BinaryOperator):
    __slots__ = ()
    _operator = '<'


class Greater(BinaryOperator):
    __slots__ = ()
    _operator = '>'


class LessEqual(BinaryOperator):
    __slots__ = ()
    _operator = '<='


class GreaterEqual(BinaryOperator):
    __slots__ = ()
    _operator = '>='


class Equal(BinaryOperator):
    __slots__ = ()
    _operator = '='

    @property
    def params(self):
        if self.left is None:
            return (self.right,)
        elif self.right is None:
            return (self.right,)
        return super(Equal, self).params

    def __str__(self):
        if self.left is None:
            return '(%s IS NULL)' % self.right
        elif self.right is None:
            return '(%s IS NULL)' % self.right
        return super(Equal, self).__str__()


class NotEqual(Equal):
    __slots__ = ()
    _operator = '!='

    def __str__(self):
        if self.left is None:
            return '(%s IS NOT NULL)' % self.right
        elif self.right is None:
            return '(%s IS NOT NULL)' % self.right
        return super(Equal, self).__str__()


class Add(BinaryOperator):
    __slots__ = ()
    _operator = '+'


class Sub(BinaryOperator):
    __slots__ = ()
    _operator = '-'


class Mul(BinaryOperator):
    __slots__ = ()
    _operator = '*'


class FloorDiv(BinaryOperator):
    __slots__ = ()
    _operator = '/'


class Mod(BinaryOperator):
    __slots__ = ()
    _operator = '%'


class Pow(BinaryOperator):
    __slots__ = ()
    _operator = '^'


class SquareRoot(UnaryOperator):
    __slots__ = ()
    _operator = '|/'


class CubeRoot(UnaryOperator):
    __slots__ = ()
    _operator = '||/'


class Factorial(UnaryOperator):
    __slots__ = ()
    _operator = '!!'


class Abs(UnaryOperator):
    __slots__ = ()
    _operator = '@'


class BAnd(BinaryOperator):
    __slots__ = ()
    _operator = '&'


class BOr(BinaryOperator):
    __slots__ = ()
    _operator = '|'


class BXor(BinaryOperator):
    __slots__ = ()
    _operator = '#'


class BNot(UnaryOperator):
    __slots__ = ()
    _operator = '~'


class LShift(BinaryOperator):
    __slots__ = ()
    _operator = '<<'


class RShift(BinaryOperator):
    __slots__ = ()
    _operator = '>>'


class Concat(BinaryOperator):
    __slots__ = ()
    _operator = '||'


class Like(BinaryOperator):
    __slots__ = ()
    _operator = 'LIKE'


class NotLike(BinaryOperator):
    __slots__ = ()
    _operator = 'NOT LIKE'


class ILike(BinaryOperator):
    __slots__ = ()

    @property
    def _operator(self):
        if Flavor.get().ilike:
            return 'ILIKE'
        else:
            return 'LIKE'


class NotILike(BinaryOperator):
    __slots__ = ()

    @property
    def _operator(self):
        if Flavor.get().ilike:
            return 'NOT ILIKE'
        else:
            return 'NOT LIKE'

# TODO SIMILAR


class In(BinaryOperator):
    __slots__ = ()
    _operator = 'IN'


class NotIn(BinaryOperator):
    __slots__ = ()
    _operator = 'NOT IN'


class Exists(UnaryOperator):
    __slots__ = ()
    _operator = 'EXISTS'


class Any(UnaryOperator):
    __slots__ = ()
    _operator = 'ANY'

Some = Any


class All(UnaryOperator):
    __slots__ = ()
    _operator = 'ALL'


_INVERT = {
    Less: GreaterEqual,
    Greater: LessEqual,
    LessEqual: Greater,
    GreaterEqual: Less,
    Equal: NotEqual,
    NotEqual: Equal,
    Like: NotLike,
    NotLike: Like,
    ILike: NotILike,
    NotILike: ILike,
    In: NotIn,
    NotIn: In,
    }
