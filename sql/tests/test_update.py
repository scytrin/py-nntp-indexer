#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table


class TestUpdate(unittest.TestCase):
    table = Table('t')

    def test_update1(self):
        query = self.table.update([self.table.c], ['foo'])
        self.assertEqual(str(query), 'UPDATE "t" AS "a" SET ("c") = (%s)')
        self.assertEqual(query.params, ('foo',))

        query.where = (self.table.b == True)
        self.assertEqual(str(query),
            'UPDATE "t" AS "a" SET ("c") = (%s) WHERE ("a"."b" = %s)')
        self.assertEqual(query.params, ('foo', True))

    def test_update2(self):
        t1 = Table('t1')
        t2 = Table('t2')
        query = t1.update([t1.c], ['foo'], from_=[t2], where=(t1.c == t2.c))
        self.assertEqual(str(query),
            'UPDATE "t1" AS "b" SET ("c") = (%s) FROM "t2" AS "a" '
            'WHERE ("b"."c" = "a"."c")')
        self.assertEqual(query.params, ('foo',))
