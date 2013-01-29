#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table
from sql.operators import And, Not, Less


class TestOperators(unittest.TestCase):
    table = Table('t')

    def test_and(self):
        and_ = And((self.table.c1, self.table.c2))
        self.assertEqual(str(and_), '("c1" AND "c2")')

    def test_not(self):
        not_ = Not(self.table.c)
        self.assertEqual(str(not_), '(NOT "c")')

    def test_less(self):
        less = Less(self.table.c1, self.table.c2)
        self.assertEqual(str(less), '("c1" < "c2")')
