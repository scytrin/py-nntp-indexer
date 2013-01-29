#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table


class TestInsert(unittest.TestCase):
    table = Table('t')

    def test_insert1(self):
        query = self.table.insert()
        self.assertEqual(str(query), 'INSERT INTO "t" DEFAULT VALUES')
        self.assertEqual(query.params, ())

    def test_insert2(self):
        query = self.table.insert([self.table.c1, self.table.c2],
            ['foo', 'bar'])
        self.assertEqual(str(query),
            'INSERT INTO "t" ("c1", "c2") VALUES (%s, %s)')
        self.assertEqual(query.params, ('foo', 'bar'))

    def test_insert3(self):
        query = self.table.insert([self.table.c1, self.table.c2],
            [['foo', 'bar'], ['spam', 'eggs']])
        self.assertEqual(str(query),
            'INSERT INTO "t" ("c1", "c2") VALUES (%s, %s), (%s, %s)')
        self.assertEqual(query.params, ('foo', 'bar', 'spam', 'eggs'))

    def test_insert4(self):
        t1 = Table('t1')
        t2 = Table('t2')
        subquery = t2.select(t2.c1, t2.c2)
        query = t1.insert([t1.c1, t1.c2], subquery)
        self.assertEqual(str(query),
            'INSERT INTO "t1" ("c1", "c2") VALUES '
            '(SELECT "a"."c1", "a"."c2" FROM "t2" AS "a")')
        self.assertEqual(query.params, ())
