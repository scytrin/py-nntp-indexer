#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table, Flavor
from sql.functions import Function, Abs, Overlay, Trim, AtTimeZone


class TestFunctions(unittest.TestCase):
    table = Table('t')

    def test_abs(self):
        abs_ = Abs(self.table.c1)
        self.assertEqual(str(abs_), 'ABS("c1")')
        self.assertEqual(abs_.params, ())

        abs_ = Abs(-12)
        self.assertEqual(str(abs_), 'ABS(-12)')
        self.assertEqual(abs_.params, ())

    def test_mapping(self):
        class MyAbs(Function):
            _function = 'MY_ABS'
            params = ('test',)

        abs_ = Abs(self.table.c1)
        flavor = Flavor(function_mapping={
                Abs: MyAbs,
                })
        Flavor.set(flavor)
        try:
            self.assertEqual(str(abs_), 'MY_ABS("c1")')
            self.assertEqual(abs_.params, ('test',))
        finally:
            Flavor.set(Flavor())

    def test_overlay(self):
        overlay = Overlay(self.table.c1, 'test', 3)
        self.assertEqual(str(overlay), 'OVERLAY("c1" PLACING %s FROM 3)')
        self.assertEqual(overlay.params, ('test',))
        overlay = Overlay(self.table.c1, 'test', 3, 7)
        self.assertEqual(str(overlay), 'OVERLAY("c1" PLACING %s FROM 3 TO 7)')
        self.assertEqual(overlay.params, ('test',))

    def test_trim(self):
        trim = Trim(' test ')
        self.assertEqual(str(trim), 'TRIM(BOTH %s FROM %s)')
        self.assertEqual(trim.params, (' ', ' test ',))

    def test_at_time_zone(self):
        time_zone = AtTimeZone(self.table.c1, 'UTC')
        self.assertEqual(str(time_zone), '"c1" AT TIME ZONE %s')
        self.assertEqual(time_zone.params, ('UTC',))
