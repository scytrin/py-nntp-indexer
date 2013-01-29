#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Join, Table


class TestJoin(unittest.TestCase):
    def test_join(self):
        t1 = Table('t1')
        t2 = Table('t2')
        join = Join(t1, t2)
        self.assertEqual(str(join), '"t1" INNER JOIN "t2"')
        self.assertEqual(join.params, ())

        join.condition = t1.c == t2.c
        self.assertEqual(str(join), '"t1" INNER JOIN "t2" ON ("c" = "c")')
