import unittest
import sys
import APIv1
import APIv2

class TestAPI(unittest.TestCase):
    VERSION = 'v1'

    def setUp(self):
        if (self.VERSION == 'v1'):
            self.app = APIv1.app.test_client()
        else:
            self.app = APIv2.app.test_client()

    def test_index(self):
        result = self.app.get('/')
        self.assertEqual(result.status_code, 200)

    def test_forecast(self):
        for interval in ['24', '48', '72']:
            result = self.app.get('/servicio/' + self.VERSION + '/prediccion/' + interval + 'horas')
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.content_type, "application/json")

    def test_no_forecast(self):
        result = self.app.get('/servicio/' + self.VERSION + '/prediccion/86horas')
        self.assertEqual(result.status_code, 400)

    def test_wrong_url(self):
        result = self.app.get('/wrong_url')
        self.assertEqual(result.status_code, 404)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TestAPI.VERSION = sys.argv.pop()
    unittest.main()
