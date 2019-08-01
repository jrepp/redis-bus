import unittest
from redisbus.client import Client
from redisbus.worker import Worker
from redisbus.worker import Arguments
from redisbus.worker import Subscription
from redisbus.utility import DictObj

class BasicsTestCase(unittest.TestCase):
    def test_client(self):
        connection_pool = {}
        site = 'test'
        log = None
        c = Client(connection_pool, site, log)
        self.assertIsNotNone(c)

    def test_worker(self):
        args = Arguments()
        worker_id = None
        w = Worker(args, worker_id)
        self.assertIsNotNone(w)

    def test_subscription(self):
        channel_names = []
        connection_pool = {}
        s = Subscription(channel_names, connection_pool)
        #self.assertRaises(ImportError, lambda: _versions.build(42))
        self.assertIsNotNone(s)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(BasicsTestCase)
