import unittest
import calc
from calc import get_api_content
import mock


class TestCalc(unittest.TestCase):

    def test_add(self):
        self.assertEqual(calc.add(10, 5), 15)
        self.assertEqual(calc.add(-1, 1), 0)
        self.assertEqual(calc.add(-1, -1), -2)

    def test_divide(self):
        self.assertEqual(calc.divide(10, 5), 2)
        self.assertEqual(calc.divide(-1, 1), -1)
        self.assertEqual(calc.divide(-1, -1), 1)
        self.assertEqual(calc.divide(5, 2), 2.5)

        with self.assertRaises(ValueError):
            calc.divide(10, 0)

    @mock.patch('calc.get_api_content')
    def test_get_emojis_api_url(self, get_api_content):
        get_api_content.return_value = {'emojis_url': 'xxx', 'users': 'yyy'}
        result = calc.get_emojis_api_url()
        expected_result = ['xxx']
        self.assertItemsEqual(result, expected_result)
        self.assertIsInstance(result, list)

    @mock.patch('calc.get_api_content')
    def test_get_emojis_api_url_if_empty_call(self, get_api_content):
        get_api_content.return_value = {}
        result = calc.get_emojis_api_url()
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()