# -*- coding: utf-8 -*-
import doctest

def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite('chorde.clients.inproc'))
    return tests
