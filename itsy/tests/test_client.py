from unittest import TestCase

from itsy.client import Client


class TestClient(TestCase):

    def test_proxies(self):
        proxies = {
            'http': '192.155.83.138:8888'
        }
        client = Client(proxies=proxies)

        body = client.get('http://www.cartlogic.com/ip', None)
        proxy_ip = proxies['http'].split(':', 1)[0]
        self.assertIn(proxy_ip, body)
