#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Asc, Desc, Column, Table


class TestOrder(unittest.TestCase):
    column = Column(Table('t'), 'c')

    def test_asc(self):
        self.assertEqual(str(Asc(self.column)), '"c" ASC')

    def test_desc(self):
        self.assertEqual(str(Desc(self.column)), '"c" DESC')
