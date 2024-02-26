import unittest
import redis
from redisbus.client import Client
from redisbus.worker import Worker
from redisbus.worker import Arguments
from redisbus.worker import Subscription
from redisbus.config import DefaultConfig


class BasicsTestCase(unittest.TestCase):
    def test_client(self):
        config = DefaultConfig()
        connection_pool = {}
        site = 'test'
        log = None
        c = Client(connection_pool, site, log)
        self.assertIsNotNone(c)

    def test_worker(self):
        args = Arguments(DefaultConfig())
        w = Worker(args, worker_id=None)
        self.assertIsNotNone(w)

    def test_subscription(self):
        channel_names = []
        config = DefaultConfig()
        connection_pool = redis.ConnectionPool(
            host=config.globals['redis_hostname'],
            port=config.globals['redis_port'])
        s = Subscription(channel_names, connection_pool)
        self.assertIsNotNone(s)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(BasicsTestCase)
