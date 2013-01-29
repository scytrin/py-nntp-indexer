#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table
from sql.aggregate import Avg


class TestAggregate(unittest.TestCase):
    table = Table('t')

    def test_avg(self):
        avg = Avg(self.table.c)
        self.assertEqual(str(avg), 'AVG("c")')
