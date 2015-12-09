#!/bin/env python2.6
import unittest
import agent2
import time

class UnitTestParallel(unittest.TestCase):
    def setUp(self):
        time.sleep(3)
        pass

    def tearDown(self):
        pass

    def test_return_a(self):
        self.assertEqual(agent2.return_a(), 'a')

    def test_return_b(self):
        self.assertEqual(agent2.return_b(), 'b')
 
    def test_return_c(self):
        self.assertEqual(agent2.return_c(), 'c')

    def test_return_d(self):
        self.assertEqual(agent2.return_d(), 'd')

if __name__ == '__main__':
    unittest.main()
