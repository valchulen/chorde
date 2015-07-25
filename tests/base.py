# -*- coding: utf-8 -*-

# Nose compatibility
import unittest

class TestCase(unittest.TestCase):
    def run(self, result):
        if not hasattr(result, 'addUnexpectedSuccess'):
            result.addUnexpectedSuccess = result.addSuccess
        super(TestCase, self).run(result)

