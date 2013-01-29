#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table
from sql.conditionals import Case, Coalesce, NullIf, Greatest, Least


class TestConditionals(unittest.TestCase):
    table = Table('t')

    def test_case(self):
        case = Case((self.table.c1, 'foo'),
            (self.table.c2, 'bar'),
            else_=self.table.c3)
        self.assertEqual(str(case),
            'CASE WHEN "c1" THEN %s '
            'WHEN "c2" THEN %s '
            'ELSE "c3" END')
        self.assertEqual(case.params, ('foo', 'bar'))

    def test_coalesce(self):
        coalesce = Coalesce(self.table.c1, self.table.c2, 'foo')
        self.assertEqual(str(coalesce), 'COALESCE("c1", "c2", %s)')
        self.assertEqual(coalesce.params, ('foo',))

    def test_nullif(self):
        nullif = NullIf(self.table.c1, 'foo')
        self.assertEqual(str(nullif), 'NULLIF("c1", %s)')
        self.assertEqual(nullif.params, ('foo',))

    def test_greatest(self):
        greatest = Greatest(self.table.c1, self.table.c2, 'foo')
        self.assertEqual(str(greatest), 'GREATEST("c1", "c2", %s)')
        self.assertEqual(greatest.params, ('foo',))

    def test_least(self):
        least = Least(self.table.c1, self.table.c2, 'foo')
        self.assertEqual(str(least), 'LEAST("c1", "c2", %s)')
        self.assertEqual(least.params, ('foo',))
