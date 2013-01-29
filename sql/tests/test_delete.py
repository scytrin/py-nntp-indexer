#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table


class TestDelete(unittest.TestCase):
    table = Table('t')

    def test_delete1(self):
        query = self.table.delete()
        self.assertEqual(str(query), 'DELETE FROM "t"')
        self.assertEqual(query.params, ())

    def test_delete2(self):
        query = self.table.delete(where=(self.table.c == 'foo'))
        self.assertEqual(str(query), 'DELETE FROM "t" WHERE ("c" = %s)')
        self.assertEqual(query.params, ('foo',))

    def test_delete3(self):
        t1 = Table('t1')
        t2 = Table('t2')
        query = t1.delete(where=(t1.c.in_(t2.select(t2.c))))
        self.assertEqual(str(query),
            'DELETE FROM "t1" WHERE ("c" IN ('
            'SELECT "a"."c" FROM "t2" AS "a"))')
        self.assertEqual(query.params, ())
