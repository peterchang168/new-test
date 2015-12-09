#!/bin/env python2.6
import unittest
import new_domain_redis_agent
import time

class UnitTestParallel(unittest.TestCase):
    def setUp(self):
        time.sleep(3)
        pass

    def tearDown(self):
        pass

    def test_return_a(self):
        self.assertEqual(new_domain_redis_agent.return_a(), 'a')

    def test_return_b(self):
        self.assertEqual(new_domain_redis_agent.return_b(), 'b')
 
    def test_return_c(self):
        self.assertEqual(new_domain_redis_agent.return_c(), 'c')

    def test_return_d(self):
        self.assertEqual(new_domain_redis_agent.return_d(), 'd')

if __name__ == '__main__':
    unittest.main()
