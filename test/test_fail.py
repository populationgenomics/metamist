import unittest

class FailTests(unittest.TestCase):
    def test_1(self):
        self.assertTrue(False, 'Default test')
