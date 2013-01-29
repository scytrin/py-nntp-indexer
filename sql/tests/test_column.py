#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Column, Table, AliasManager


class TestColumn(unittest.TestCase):
    def test_column(self):
        column = Column(Table('t'), 'c')
        self.assertEqual(str(column), '"c"')

        with AliasManager():
            self.assertEqual(str(column), '"a"."c"')
