import asyncio
import os
import re
import unittest

from functools import wraps
from aioredis import create_redis, create_connection, create_pool, \
    create_sentinel


REDIS_VERSION = os.environ.get('REDIS_VERSION')
if not REDIS_VERSION:
    REDIS_VERSION = (0, 0, 0)
else:
    res = re.findall('(\d\.\d\.\d+)', REDIS_VERSION)
    if res:
        REDIS_VERSION = tuple(map(int, res[0].split('.')))
    else:
        REDIS_VERSION = (0, 0, 0)


def run_until_complete(fun_or_timeout):
    if isinstance(fun_or_timeout, int):
        timeout = fun_or_timeout

        def deco(fun):
            if not asyncio.iscoroutinefunction(fun):
                fun = asyncio.coroutine(fun)

            @wraps(fun)
            def wrapper(test, *args, **kw):
                loop = test.loop
                ret = loop.run_until_complete(
                    asyncio.wait_for(fun(test, *args, **kw), timeout,
                                     loop=loop))
                return ret
        return deco
    else:
        fun = fun_or_timeout
        if not asyncio.iscoroutinefunction(fun):
            fun = asyncio.coroutine(fun)

        @wraps(fun)
        def wrapper(test, *args, **kw):
            timeout = 15
            loop = test.loop
            ret = loop.run_until_complete(
                asyncio.wait_for(fun(test, *args, **kw), timeout, loop=loop))
            return ret

        return wrapper


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.redis_port = int(os.environ.get('REDIS_PORT') or 6379)
        socket = os.environ.get('REDIS_SOCKET')
        self.redis_socket = socket or '/tmp/aioredis.sock'
        self._conns = []
        self._redises = []
        self._pools = []

    def tearDown(self):
        waiters = []
        while self._conns:
            conn = self._conns.pop(0)
            conn.close()
            waiters.append(conn.wait_closed())
        while self._redises:
            redis = self._redises.pop(0)
            redis.close()
            waiters.append(redis.wait_closed())
        while self._pools:
            pool = self._pools.pop(0)
            waiters.append(pool.clear())
        if waiters:
            self.loop.run_until_complete(
                asyncio.gather(*waiters, loop=self.loop))
        self.loop.close()
        del self.loop

    @asyncio.coroutine
    def create_connection(self, *args, **kw):
        conn = yield from create_connection(*args, **kw)
        self._conns.append(conn)
        return conn

    @asyncio.coroutine
    def create_redis(self, *args, **kw):
        redis = yield from create_redis(*args, **kw)
        self._redises.append(redis)
        return redis

    @asyncio.coroutine
    def create_sentinel(self, *args, **kw):
        redis = yield from create_sentinel(*args, **kw)
        self._redises.append(redis)
        return redis

    @asyncio.coroutine
    def create_pool(self, *args, **kw):
        pool = yield from create_pool(*args, **kw)
        self._pools.append(pool)
        return pool


class RedisTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(self.create_redis(
            ('localhost', self.redis_port), loop=self.loop))

    def tearDown(self):
        del self.redis
        super().tearDown()

    @asyncio.coroutine
    def add(self, key, value):
        ok = yield from self.redis.connection.execute('set', key, value)
        self.assertEqual(ok, b'OK')

    @asyncio.coroutine
    def flushall(self):
        ok = yield from self.redis.connection.execute('flushall')
        self.assertEqual(ok, b'OK')


class RedisEncodingTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(self.create_redis(
            ('localhost', self.redis_port), loop=self.loop, encoding='utf-8'))

    def tearDown(self):
        del self.redis
        super().tearDown()


class RedisSentinelTest(BaseTest):
    sentinel_ip = os.environ.get('SENTINEL_IP', 'localhost')
    sentinel_port = int(os.environ.get('SENTINEL_PORT') or 26379)
    master_name = os.environ.get("SENTINEL_NAME", 'mymaster')

    def setUp(self):
        super().setUp()
        self.redis_sentinel = self.loop.run_until_complete(
            self.create_sentinel([(self.sentinel_ip, self.sentinel_port)],
                                 loop=self.loop, encoding='utf-8'))

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        loop = asyncio.new_event_loop()
        run = loop.run_until_complete
        redis_port = int(os.environ.get("REDIS_PORT") or 6379)

        @asyncio.coroutine
        def setup(loop):
            sentinel = yield from create_connection(
                (cls.sentinel_ip, cls.sentinel_port), loop=loop)
            yield from sentinel.execute(
                'sentinel', 'monitor', cls.master_name,
                '127.0.0.1', redis_port, 1)
            master = yield from create_redis(
                ('localhost', redis_port), loop=loop)
            slave = yield from create_redis(
                ('localhost', 6380), loop=loop)
            yield from master.config_set('slave-read-only', 'yes')
            yield from slave.config_set('slave-read-only', 'yes')
            yield from slave.slaveof('localhost', redis_port)

            @asyncio.coroutine
            def wait_ready(word, *args):
                while True:
                    res = yield from sentinel.execute(
                        'sentinel', '{}s'.format(word), *args,
                        encoding='utf-8')
                    for row in res:
                        it = iter(row)
                        if dict(zip(it, it))['flags'] == word:
                            return
            yield from asyncio.wait_for(
                asyncio.gather(wait_ready('master'),
                               wait_ready('slave', cls.master_name),
                               loop=loop),
                20, loop=loop)

            sentinel.close()
            slave.close()
            yield from sentinel.wait_closed()
            yield from slave.wait_closed()

        run(asyncio.wait_for(setup(loop), 20, loop=loop))
        loop.close()
        del loop

    @asyncio.coroutine
    def get_master_connection(self):
        redis = yield from self.redis_sentinel.master_for(self.master_name,
                                                          loop=self.loop)
        self._redises.append(redis)
        return redis

    @asyncio.coroutine
    def get_slave_connection(self):
        redis = yield from self.redis_sentinel.slave_for(self.master_name,
                                                         loop=self.loop)
        self._redises.append(redis)
        return redis

    def tearDown(self):
        del self.redis_sentinel
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        loop = asyncio.new_event_loop()
        run = loop.run_until_complete

        @asyncio.coroutine
        def tear_down(loop):
            redis_port = int(os.environ.get("REDIS_PORT") or 6379)
            sentinel = yield from create_connection(
                (cls.sentinel_ip, cls.sentinel_port), loop=loop)
            master = yield from create_redis(
                ('localhost', redis_port), loop=loop)
            slave = yield from create_redis(
                ('localhost', 6380), loop=loop)
            yield from sentinel.execute('sentinel', 'remove', cls.master_name)
            yield from sentinel.execute('sentinel', 'flushconfig')
            yield from master.slaveof()
            yield from slave.slaveof()
            sentinel.close()
            master.close()
            slave.close()
            yield from asyncio.gather(
                sentinel.wait_closed(),
                master.wait_closed(),
                slave.wait_closed(),
                loop=loop)
        run(asyncio.wait_for(tear_down(loop), 20, loop=loop))
        loop.close()
        del loop
        super().tearDownClass()
