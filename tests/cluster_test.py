import asyncio
import functools
import math
import socket
from unittest import mock

import pytest

from aioredis import ProtocolError, ReplyError
from aioredis.cluster import RedisCluster, RedisPoolCluster
from aioredis.cluster.cluster import (ClusterNode, ClusterNodesManager,
                                      create_cluster, create_pool_cluster)
from aioredis.cluster.testcluster import TestCluster as Cluster
from aioredis.cluster.transaction import InvalidPipelineOperation
from aioredis.cluster.util import KnownReplyError, parse_cluster_response_error
from aioredis.commands import ContextRedis
from aioredis.commands.cluster import (parse_cluster_nodes,
                                       parse_cluster_nodes_lines,
                                       parse_cluster_slots)
from aioredis.errors import MultiExecError, PipelineError, RedisClusterError
from aioredis.util import _NOTSET
from _testutils import redis_version

RAW_SLAVE_INFO_DATA = b"""\
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006 slave \
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
"""

# example from the CLUSTER NODES doc
RAW_NODE_INFO_DATA_OK = b"""
07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004 slave \
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master \
- 0 1426238316232 2 connected 5461-10922 \
[10925-<-292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f] \
[5461->-292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f]
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master \
- 0 1426238318243 3 connected 0 10925-16383 \
[10924->-67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1] \
[10925->-67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1]
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005 slave \
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@30001 \
my,master - 0 0 1 connected 1-5460 10923-10924
""" + RAW_SLAVE_INFO_DATA

RAW_NODE_INFO_DATA_FAIL = """
07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004 slave,fail \
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master,fail? \
- 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master \
- 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005 slave \
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006 slave \
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@30001 \
my,master - 0 0 1 connected 0-5460
"""

NODE_INFO_DATA_OK = [
    {
        'id': b'07c37dfeb235213a872192d90877d0cd55635b91',
        'host': b'127.0.0.1',
        'port': 30004,
        'nat-port': None,
        'flags': (b'slave',),
        'master': b'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca',
        'ping-sent': 0,
        'pong-recv': 1426238317239,
        'config_epoch': 4,
        'status': b'connected',
        'slots': tuple(),
        'migrations': tuple()
    },
    {
        'id': b'67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1',
        'host': b'127.0.0.1',
        'port': 30002,
        'nat-port': None,
        'flags': (b'master',),
        'master': None,
        'ping-sent': 0,
        'pong-recv': 1426238316232,
        'config_epoch': 2,
        'status': b'connected',
        'slots': ((5461, 10922),),
        'migrations': (
            {
                'node_id': b'292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f',
                'slot': 10925,
                'state': b'importing'
            },
            {
                'node_id': b'292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f',
                'slot': 5461,
                'state': b'migrating'
            },
        )
    },
    {
        'id': b'292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f',
        'host': b'127.0.0.1',
        'port': 30003,
        'nat-port': None,
        'flags': (b'master',),
        'master': None,
        'ping-sent': 0,
        'pong-recv': 1426238318243,
        'config_epoch': 3,
        'status': b'connected',
        'slots': ((0, 0), (10925, 16383)),
        'migrations': (
            {
                'node_id': b'67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1',
                'slot': 10924,
                'state': b'migrating'
            },
            {
                'node_id': b'67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1',
                'slot': 10925,
                'state': b'migrating'
            },
        )
    },
    {
        'id': b'6ec23923021cf3ffec47632106199cb7f496ce01',
        'host': b'127.0.0.1',
        'port': 30005,
        'nat-port': None,
        'flags': (b'slave',),
        'master': b'67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1',
        'ping-sent': 0,
        'pong-recv': 1426238316232,
        'config_epoch': 5,
        'status': b'connected',
        'slots': tuple(),
        'migrations': tuple()
    },
    {
        'id': b'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca',
        'host': b'127.0.0.1',
        'port': 30001,
        'nat-port': 30001,
        'flags': (b'my', b'master'),
        'master': None,
        'ping-sent': 0,
        'pong-recv': 0,
        'config_epoch': 1,
        'status': b'connected',
        'slots': ((1, 5460), (10923, 10924)),
        'migrations': tuple()
    },
    {
        'id': b'824fe116063bc5fcf9f4ffd895bc17aee7731ac3',
        'host': b'127.0.0.1',
        'port': 30006,
        'nat-port': None,
        'flags': (b'slave',),
        'master': b'292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f',
        'ping-sent': 0,
        'pong-recv': 1426238317741,
        'config_epoch': 6,
        'status': b'connected',
        'slots': tuple(),
        'migrations': tuple()
    }
]

NODE_INFO_DATA_FAIL = [
    {
        'id': '07c37dfeb235213a872192d90877d0cd55635b91',
        'host': '127.0.0.1',
        'port': 30004,
        'nat-port': None,
        'flags': ('slave', 'fail'),
        'master': 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca',
        'ping-sent': 0,
        'pong-recv': 1426238317239,
        'config_epoch': 4,
        'status': 'connected',
        'slots': tuple(),
        'migrations': tuple()
    },
    {
        'id': '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1',
        'host': '127.0.0.1',
        'port': 30002,
        'nat-port': None,
        'flags': ('master', 'fail?'),
        'master': None,
        'ping-sent': 0,
        'pong-recv': 1426238316232,
        'config_epoch': 2,
        'status': 'connected',
        'slots': ((5461, 10922),),
        'migrations': tuple()
    },
    {
        'id': '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f',
        'host': '127.0.0.1',
        'port': 30003,
        'nat-port': None,
        'flags': ('master',),
        'master': None,
        'ping-sent': 0,
        'pong-recv': 1426238318243,
        'config_epoch': 3,
        'status': 'connected',
        'slots': ((10923, 16383),),
        'migrations': tuple()
    },
    {
        'id': '6ec23923021cf3ffec47632106199cb7f496ce01',
        'host': '127.0.0.1',
        'port': 30005,
        'nat-port': None,
        'flags': ('slave',),
        'master': '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1',
        'ping-sent': 0,
        'pong-recv': 1426238316232,
        'config_epoch': 5,
        'status': 'connected',
        'slots': tuple(),
        'migrations': tuple()
    },
    {
        'id': '824fe116063bc5fcf9f4ffd895bc17aee7731ac3',
        'host': '127.0.0.1',
        'port': 30006,
        'nat-port': None,
        'flags': ('slave',),
        'master': '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f',
        'ping-sent': 0,
        'pong-recv': 1426238317741,
        'config_epoch': 6,
        'status': 'connected',
        'slots': tuple(),
        'migrations': tuple()
    },
    {
        'id': 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca',
        'host': '127.0.0.1',
        'port': 30001,
        'nat-port': 30001,
        'flags': ('my', 'master'),
        'master': None,
        'ping-sent': 0,
        'pong-recv': 0,
        'config_epoch': 1,
        'status': 'connected',
        'slots': ((0, 5460),),
        'migrations': tuple()
    }
]


RAW_SLOTS_INFO = [
    [
        10922, 16383,
        ['127.0.0.1', 7008, '534daeab36b41926d3149f7e3b08c64caef2001a'],
        ['127.0.0.1', 7014, '88da62baa1404580936b04d4e601889acf1096bf']],
    [
        0, 5460,
        ['127.0.0.1', 7000, '89e2e5155998dbb93ae759a0c7293d312f7b2be6'],
        ['127.0.0.1', 7009, '4236cd00bf46ffc4718479a766a8c8452d55cd17']],
    [
        5461, 10921,
        ['127.0.0.1', 7007, '64c208c8830b049b562972cb300a0b63ed20fe96'],
        ['127.0.0.1', 7013, '617509170350ee7c6190af965ed079296e98cfb8']
    ]
]

SLOTS_INFO = {
    (0, 5460): ('127.0.0.1', 7000),
    (5461, 10921): ('127.0.0.1', 7007),
    (10922, 16383): ('127.0.0.1', 7008)
}


SLOT_ZERO_KEY = 'key:24358'  # is mapped to keyslot 0
KEY_KEY_SLOT = 12539
NODES_COUNT = 6
DESIRE_START_PORT = 7000


class FakeConnection:
    def __init__(self,
                 port,
                 loop,
                 return_value=b'OK',
                 side_effect=None,
                 encoding='utf-8'):
        self.port = port
        self.was_used = False
        self.encoding = encoding
        self.return_value = return_value
        self.side_effect = side_effect
        self.loop = loop
        self.closed = False

    def close(self):
        pass

    @asyncio.coroutine
    def wait_closed(self):
        pass

    def __getattr__(self, item):
        # side_effect takes precedence over return_value
        if self.side_effect is None:
            future = asyncio.Future(loop=self.loop)
            if isinstance(self.return_value, Exception):
                future.set_exception(self.return_value)
            else:
                future.set_result(self.return_value)

            result = mock.Mock(return_value=future)
        else:
            if isinstance(self.side_effect, Exception):
                future = asyncio.Future(loop=self.loop)
                future.set_exception(self.side_effect)
                result = mock.Mock(return_value=future)
            elif isinstance(self.side_effect, list):
                futures = []
                for res in self.side_effect:
                    future = asyncio.Future(loop=self.loop)
                    if isinstance(res, Exception):
                        future.set_exception(res)
                    else:
                        future.set_result(res)
                    futures.append(future)
                result = mock.Mock(side_effect=futures)
            elif callable(self.side_effect):
                result = mock.Mock(side_effect=self.side_effect)
            else:
                future = asyncio.Future(loop=self.loop)
                future.set_result(self.side_effect)
                result = mock.Mock(return_value=future)
        setattr(self, item, result)
        return result


class CreateConnectionMock:
    def __init__(self, connections):
        assert isinstance(connections, dict)
        self.connections = connections
        self.contextManager = mock.patch(
            'aioredis.commands.create_connection',
            side_effect=self.get_fake_connection
        )

    async def get_fake_connection(
            self, address, db, password, ssl, encoding, loop, **kwargs
    ):
        host, port = address
        assert host == '127.0.0.1'
        expected_connection = self.connections[port]
        expected_connection.was_used = True
        assert db == 0
        assert password is None
        assert encoding == 'utf-8'
        return expected_connection

    def __enter__(self):
        self.contextManager.__enter__()
        return self

    def __exit__(self, *args):
        self.contextManager.__exit__(*args)
        assert all(
            connection.was_used
            for connection in self.connections.values()
        )


class PoolConnectionMock:
    def __init__(self, cluster, loop, connections):
        self.cluster = cluster
        self.connections = connections
        self.contextManagers = []

        async def create_connection_future(port):
            connection = self.connections[port]
            connection.was_used = True
            return ContextRedis(connection)

        async def create_error_future(port):
            raise AssertionError(
                'No connection expected for port {}.'.format(port)
            )

        for pool in cluster._cluster_pool.values():
            port = pool.address[1]
            if port in self.connections:
                create_future = functools.partial(
                    create_connection_future, port
                )
            else:
                create_future = functools.partial(create_error_future, port)

            self.contextManagers.append(mock.patch.object(
                pool._pool_or_conn, 'acquire', side_effect=create_future
            ))
            self.contextManagers.append(
                mock.patch.object(pool._pool_or_conn, 'release')
            )

    def __enter__(self):
        for manager in self.contextManagers:
            manager.__enter__()

    def __exit__(self, *args):
        for manager in reversed(self.contextManagers):
            manager.__exit__(*args)

        assert all(
            connection.was_used for connection in self.connections.values()
        )


@pytest.fixture(scope='module')
def free_ports():
    ports = []
    current_port = DESIRE_START_PORT
    while len(ports) < NODES_COUNT:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('127.0.0.1', current_port))
            except socket.error as e:
                if e.errno == socket.errno.EADDRINUSE:
                    current_port += 1
                    continue
                raise

            ports.append(current_port)
            current_port += 1

    return ports


@pytest.fixture
def cluster_server(server_bin, free_ports, tmpdir):
    yield from _cluster_server(
        server_bin, free_ports, tmpdir, assign_slots=True)


@pytest.fixture
def cluster_server_no_slots_assigned(server_bin, free_ports, tmpdir):
    yield from _cluster_server(
        server_bin, free_ports, tmpdir, assign_slots=False)


def _cluster_server(server_bin, free_ports, tmpdir, assign_slots):
    cluster_directory = tmpdir.mkdir('redisclustertest')
    server = Cluster(
        free_ports,
        str(cluster_directory),
        server_exec=server_bin,
        assign_slots=assign_slots
    )
    server.setup()

    yield server

    server.terminate()
    server.clear_directories()


@pytest.fixture
def nodes(free_ports):
    return [
        ('127.0.0.1', port)
        for port in free_ports
    ]


@pytest.fixture
def choice_mock():
    patcher = mock.patch("random.choice")

    patch = patcher.start()
    yield patch
    patcher.stop()


@pytest.fixture
def test_cluster(loop, nodes, cluster_server):
    return loop.run_until_complete(
        create_cluster(nodes, encoding='utf-8', loop=loop)
    )


@pytest.fixture
def test_cluster_no_slots_assigned(
        loop, nodes, cluster_server_no_slots_assigned):
    return loop.run_until_complete(
        create_cluster(nodes, encoding='utf-8', loop=loop)
    )


@pytest.fixture
def test_pool_cluster(loop, nodes, cluster_server):
    pool_cluster = loop.run_until_complete(
        create_pool_cluster(nodes, encoding='utf-8', loop=loop)
    )

    yield pool_cluster

    loop.run_until_complete(pool_cluster.clear())


@pytest.fixture
def key_and_slot(test_cluster, loop):
    loop.run_until_complete(test_cluster.set('key', 'value'))

    yield 'key', KEY_KEY_SLOT

    loop.run_until_complete(test_cluster.delete('key'))


@pytest.fixture
def zero_slot_key(test_cluster, loop):
    loop.run_until_complete(test_cluster.set(SLOT_ZERO_KEY, 'value'))

    yield SLOT_ZERO_KEY

    loop.run_until_complete(test_cluster.delete(SLOT_ZERO_KEY))


async def _wait_result(func, attempts=60, sleep_time=0.5, **kwargs):
    attempts_count = 0
    while attempts_count < attempts:
        try:
            return await func(**kwargs)

        except AssertionError:
            if attempts_count >= attempts:
                raise

        attempts_count += 1
        await asyncio.sleep(sleep_time)

    assert False


cluster_test = redis_version(
    3, 0, 0, reason='Cluster support was added in version 3')


def test_parse_cluster_response_error():
    assert parse_cluster_response_error(ReplyError('')) is None
    assert parse_cluster_response_error(
        ReplyError('ASK 3999 127.0.0.1:6381')
    ) == KnownReplyError(reply="ASK", args=("127.0.0.1", 6381))
    assert parse_cluster_response_error(
        ReplyError('MOVED 3999 127.0.0.1:6381')
    ) == KnownReplyError(reply="MOVED", args=("127.0.0.1", 6381))


def test_nodes_ok_info_parse():
    data = list(parse_cluster_nodes(RAW_NODE_INFO_DATA_OK))
    assert data == NODE_INFO_DATA_OK


def test_nodes_fail_info_parse():
    data = list(parse_cluster_nodes(RAW_NODE_INFO_DATA_FAIL, encoding='utf-8'))
    assert data == NODE_INFO_DATA_FAIL


def test_slave_info_lines_parse():
    data = list(parse_cluster_nodes_lines([RAW_SLAVE_INFO_DATA]))

    assert data == [NODE_INFO_DATA_OK[-1]]


def test_slots_info_parse():
    data = dict(parse_cluster_slots(RAW_SLOTS_INFO))
    assert data == SLOTS_INFO


def test_key_slot():
    assert ClusterNodesManager.key_slot(SLOT_ZERO_KEY) == 0
    assert ClusterNodesManager.key_slot('key') == KEY_KEY_SLOT
    assert ClusterNodesManager.key_slot(b'key') == KEY_KEY_SLOT


def test_create():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_FAIL)
    assert len(manager.nodes) == 6
    assert all(isinstance(node, ClusterNode) for node in manager.nodes)


def test_node_count():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_FAIL)
    assert manager.nodes_count == 4
    assert manager.masters_count == 2
    assert manager.slaves_count == 2


def test_alive_nodes():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_FAIL)
    assert manager.alive_nodes == manager.nodes[2:]


def test_cluster_node():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_FAIL)
    node1 = manager.nodes[0]
    assert not node1.is_master
    assert node1.is_slave
    assert node1.address == ('127.0.0.1', 30004)
    assert not node1.is_alive

    node2 = manager.nodes[2]
    assert node2.is_master
    assert not node2.is_slave
    assert node2.is_alive

    assert node1 == manager.get_node_by_id(node1.id)


def test_in_range():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_FAIL)
    master = manager.nodes[5]
    assert master.in_range(0)
    assert master.in_range(5460)
    assert not master.in_range(5461)

    master.slots = tuple()

    assert not master.in_range(0)


def test_all_slots_covered():
    decoded_node_info_ok = list(parse_cluster_nodes(
        RAW_NODE_INFO_DATA_OK.decode('utf-8'), encoding='utf-8'
    ))
    manager = ClusterNodesManager.create(decoded_node_info_ok)
    assert manager.all_slots_covered

    manager = ClusterNodesManager.create(NODE_INFO_DATA_FAIL)
    assert not manager.all_slots_covered

    decoded_node_info_ok[2]['slots'] = list(decoded_node_info_ok[2]['slots'])
    del decoded_node_info_ok[2]['slots'][0]

    manager = ClusterNodesManager.create(decoded_node_info_ok)
    assert not manager.all_slots_covered


def test_determine_slot():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_OK)
    assert manager.determine_slot('key') == 12539

    with pytest.raises(TypeError):
        manager.determine_slot(None)


def test_determine_slot_multiple():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_OK)
    assert manager.determine_slot('{key}:1', '{key}:2') == 12539


def test_determine_slot_multiple_different():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_OK)
    with pytest.raises(RedisClusterError):
        manager.determine_slot('key:1', 'key:2')


def test_get_node_by_id():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_FAIL)
    node = manager.get_node_by_id('07c37dfeb235213a872192d90877d0cd55635b91')
    assert node.address == ('127.0.0.1', 30004)

    no_node = manager.get_node_by_id('xxx')
    assert no_node is None


def test_get_node_by_address():
    manager = ClusterNodesManager.create(NODE_INFO_DATA_FAIL)
    node = manager.get_node_by_address(('127.0.0.1', 30004))
    assert node.id == '07c37dfeb235213a872192d90877d0cd55635b91'

    no_node = manager.get_node_by_address('xxx')
    assert no_node is None


@cluster_test
@pytest.mark.run_loop
async def test_create_cluster(test_cluster):
    assert isinstance(test_cluster, RedisCluster)

    with pytest.raises(RedisClusterError):
        await create_cluster('abc')


@cluster_test
@pytest.mark.run_loop
async def test_create_fails(loop, nodes, free_ports):
    expected_connections = {
        port: FakeConnection(
            port,
            loop,
            return_value=ProtocolError('Intentional error')
        )
        for port in free_ports
    }

    with CreateConnectionMock(expected_connections):
        with pytest.raises(RedisClusterError):
            await create_cluster(nodes, encoding='utf-8', loop=loop)


@cluster_test
@pytest.mark.run_loop
async def test_counts(test_cluster):
    assert test_cluster.node_count() == NODES_COUNT
    assert test_cluster.masters_count() == NODES_COUNT / 2
    assert test_cluster.slave_count() == NODES_COUNT / 2


@cluster_test
@pytest.mark.run_loop
async def test_get_node(test_cluster, free_ports):
    # Compare script used to setup the test cluster
    node = test_cluster.get_node('GET', 'key:0')
    assert node.address[1] == free_ports[0]

    node = test_cluster.get_node('GET', b'key:1')
    assert node.address[1] == free_ports[1]

    node = test_cluster.get_node('GET', b'{key:1}')
    assert node.address[1] == free_ports[1]

    node = test_cluster.get_node('GET', b'key:3', 'more', 'args')
    assert node.address[1] == free_ports[2]

    # Check that we have random node
    node = test_cluster.get_node('info')
    assert node.id
    assert node.address[1] in free_ports


@cluster_test
def test_cluster_all_slots_covered(test_cluster):
    assert test_cluster.all_slots_covered


@cluster_test
@pytest.mark.run_loop
async def test_get_node_eval(test_cluster, free_ports):
    node = test_cluster.get_node(
        'EVAL', keys=['{key}:1', '{key}:2'], args=['more', 'args'])
    assert node.address[1] == free_ports[2]

    with pytest.raises(RedisClusterError):
        test_cluster.get_node(
            'EVAL', keys=['keys', 'in', 'different', 'slots']
        )

    with pytest.raises(TypeError):
        test_cluster.get_node(b'EVAL', keys=123)


@cluster_test
@pytest.mark.run_loop
async def test_execute(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with CreateConnectionMock({free_ports[0]: expected_connection}):
        ok = await test_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_execute_with_moved(loop, test_cluster, free_ports):
    expected_connections = {
        free_ports[0]: FakeConnection(
            free_ports[0],
            loop,
            return_value=ReplyError(
                'MOVED 6000 127.0.0.1:{}'.format(free_ports[1])
            )
        ),
        free_ports[1]: FakeConnection(free_ports[1], loop)
    }
    with CreateConnectionMock(expected_connections):
        ok = await test_cluster.execute('SET', SLOT_ZERO_KEY, 'value')
        # This should be cached as going to free_ports[1]
        ok_2 = await test_cluster.execute("SET", SLOT_ZERO_KEY, "value")

    assert ok
    assert ok_2

    expected_connections[free_ports[0]].execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )
    expected_connections[free_ports[1]].execute.assert_has_calls([
        mock.call(b'SET', SLOT_ZERO_KEY, 'value'),
        mock.call(b'SET', SLOT_ZERO_KEY, 'value'),
    ])



@cluster_test
@pytest.mark.run_loop
async def test_execute_with_asking_param(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with CreateConnectionMock({free_ports[0]: expected_connection}):
        ok = await test_cluster.execute(
            'SET',
            SLOT_ZERO_KEY,
            'value',
            address=("127.0.0.1", free_ports[0]),
            asking=True,
        )

    assert ok

    expected_connection.execute.assert_has_calls([
        mock.call(b'ASKING'),
        mock.call(b'SET', SLOT_ZERO_KEY, 'value'),
    ])


@cluster_test
@pytest.mark.run_loop
async def test_execute_with_ask(loop, test_cluster, free_ports):
    expected_connections = {
        free_ports[0]: FakeConnection(
            free_ports[0],
            loop,
            return_value=ReplyError(
                'ASK 6000 127.0.0.1:{}'.format(free_ports[1])
            )
        ),
        free_ports[1]: FakeConnection(free_ports[1], loop)
    }
    with CreateConnectionMock(expected_connections):
        ok = await test_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connections[free_ports[0]].execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )
    expected_connections[free_ports[1]].execute.assert_has_calls([
        mock.call(b"ASKING"),
        mock.call(b'SET', SLOT_ZERO_KEY, 'value'),
    ])

@cluster_test
@pytest.mark.run_loop
async def test_execute_with_tryagain_eventually_raises(loop, test_cluster, free_ports):
    expected_connections = {
        free_ports[0]: FakeConnection(
            free_ports[0],
            loop,
            return_value=ReplyError("TRYAGAIN"),
        ),
    }
    with CreateConnectionMock(expected_connections):
        with pytest.raises(RedisClusterError) as e:
            await test_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    expected_connections[free_ports[0]].execute.assert_has_calls(
        [mock.call(b'SET', SLOT_ZERO_KEY, 'value')] * 16
    )
    assert "TTL exhausted." in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_execute_with_clusterdown_raises_exception(loop, nodes, cluster_server, free_ports):
    test_cluster = await create_cluster([nodes[0]], encoding='utf-8', loop=loop)
    expected_connections = {
        free_ports[0]: FakeConnection(
            free_ports[0],
            loop,
            return_value=ReplyError("CLUSTERDOWN"),
        ),
    }
    with CreateConnectionMock(expected_connections):
        with pytest.raises(ReplyError):
            await test_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    expected_connections[free_ports[0]].execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )
    assert test_cluster._refresh_nodes_asap
    # We should try and re-initialize after this.
    expected_connections_2 = {
        free_ports[0]: FakeConnection(
            free_ports[0],
            loop,
            return_value=ReplyError("ERROR"),
        ),
    }
    with CreateConnectionMock(expected_connections_2):
        with pytest.raises(RedisClusterError) as e:
            await test_cluster.execute("SET", SLOT_ZERO_KEY, "value")

    assert "No cluster info could be loaded from any host" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_execute_with_connection_error_tries_random_node(loop,
                                                               choice_mock,
                                                               test_cluster,
                                                               free_ports):
    # Hack random.choice to always return the host at port 7002.
    def choice_mock_side_effect(seq):
        sorted_seq = sorted(seq, key=lambda k: k.port)
        return sorted_seq[2]

    choice_mock.side_effect = choice_mock_side_effect
    expected_connections = {
        free_ports[0]: FakeConnection(
            free_ports[0],
            loop,
            return_value=ConnectionError("Uh oh!"),
        ),
        free_ports[2]: FakeConnection(
            free_ports[2],
            loop,
        ),
    }

    with CreateConnectionMock(expected_connections):
        ok = await test_cluster.execute("SET", SLOT_ZERO_KEY, "value")

    assert ok

    expected_connections[free_ports[0]].execute.assert_called_once_with(
        b"SET", SLOT_ZERO_KEY, "value"
    )
    expected_connections[free_ports[2]].execute.assert_called_once_with(
        b"SET", SLOT_ZERO_KEY, "value"
    )


@cluster_test
@pytest.mark.run_loop
async def test_execute_with_reply_error(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(
        free_ports[0], loop, return_value=ReplyError('ERROR')
    )

    with CreateConnectionMock({free_ports[0]: expected_connection}):
        with pytest.raises(ReplyError):
            await test_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_execute_with_protocol_error(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(
        free_ports[0], loop, return_value=ProtocolError('ERROR')
    )

    with CreateConnectionMock({free_ports[0]: expected_connection}):
        with pytest.raises(ProtocolError):
            await test_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_execute_many(loop, test_cluster, free_ports):
    expected_connections = {
        port: FakeConnection(port, loop)
        for port in free_ports[:3]
    }

    with CreateConnectionMock(expected_connections):
        ok = await test_cluster.execute('PING', many=True)

    assert ok == [b'OK'] * 3
    for connection in expected_connections.values():
        connection.execute.assert_called_once_with(
            'PING', encoding=mock.ANY
        )


@cluster_test
@pytest.mark.run_loop
async def test_execute_command(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with CreateConnectionMock({free_ports[0]: expected_connection}):
        ok = await test_cluster.set(SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_conn_context_given_node(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with CreateConnectionMock({free_ports[0]: expected_connection}):
        first_node = next(
            node for node in test_cluster.master_nodes
            if node.port == free_ports[0]
        )
        conn_context = await test_cluster.get_conn_context_for_node(first_node)
        async with conn_context as conn:
            ok = await conn.set(SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_conn_context_given_slot(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with CreateConnectionMock({free_ports[0]: expected_connection}):
        conn_context = await test_cluster.get_conn_context_for_slot(0)
        async with conn_context as conn:
            ok = await conn.set(SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_conn_context_raises_error(loop, test_cluster, free_ports):
    with CreateConnectionMock({}):
        with pytest.raises(RedisClusterError) as e:
            await test_cluster.get_conn_context_for_slot(20000)

    assert "No master available for slot 20000!" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_create_pool(test_pool_cluster):
    assert isinstance(test_pool_cluster, RedisPoolCluster)

    with pytest.raises(RedisClusterError):
        await create_pool_cluster('abc')


@cluster_test
@pytest.mark.run_loop
async def test_cluster_pool_get_node(test_pool_cluster, free_ports):
    node = test_pool_cluster.get_node('GET', 'key:0')
    assert node.address[1] == free_ports[0]

    node = test_pool_cluster.get_node('GET', b'key:1')
    assert node.address[1] == free_ports[1]

    node = test_pool_cluster.get_node('GET', b'key:3', 'more', 'args')
    assert node.address[1] == free_ports[2]


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute(loop, test_pool_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with PoolConnectionMock(
            test_pool_cluster, loop, {free_ports[0]: expected_connection}
    ):
        ok = await test_pool_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_with_moved(loop, test_pool_cluster, free_ports):
    expected_pool_connection = FakeConnection(
        free_ports[0],
        loop,
        return_value=ReplyError(
            'MOVED 6000 127.0.0.1:{}'.format(free_ports[1])
        )
    )
    expected_pool_connection_two = FakeConnection(free_ports[1], loop)

    with PoolConnectionMock(
            test_pool_cluster, loop,
            {
                free_ports[0]: expected_pool_connection,
                free_ports[1]: expected_pool_connection_two,
            }
    ):
        ok = await test_pool_cluster.execute('SET', SLOT_ZERO_KEY, 'value')
        # The new address should have been cached.
        ok_2 = await test_pool_cluster.execute("SET", SLOT_ZERO_KEY, "value")

    assert ok
    assert ok_2

    expected_pool_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )
    expected_pool_connection_two.execute.assert_has_calls([
        mock.call(b'SET', SLOT_ZERO_KEY, 'value'),
        mock.call(b'SET', SLOT_ZERO_KEY, 'value'),
    ])


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_with_ask(loop, test_pool_cluster, free_ports):
    expected_pool_connection = FakeConnection(
        free_ports[0],
        loop,
        return_value=ReplyError(
            'ASK 6000 127.0.0.1:{}'.format(free_ports[1])
        )
    )
    expected_pool_connection_two = FakeConnection(free_ports[1], loop)

    with PoolConnectionMock(
            test_pool_cluster, loop,
            {
                free_ports[0]: expected_pool_connection,
                free_ports[1]: expected_pool_connection_two,
            }
    ):
        ok = await test_pool_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    assert ok

    expected_pool_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )
    expected_pool_connection_two.execute.assert_has_calls([
        mock.call(b'ASKING'),
        mock.call(b'SET', SLOT_ZERO_KEY, 'value'),
    ])


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_with_tryagain_eventually_raises(loop, test_pool_cluster, free_ports):
    expected_pool_connection = FakeConnection(
        free_ports[0],
        loop,
        return_value=ReplyError("TRYAGAIN"),
    )

    with PoolConnectionMock(
            test_pool_cluster, loop,
            {
                free_ports[0]: expected_pool_connection,
            }
    ):
        with pytest.raises(RedisClusterError) as e:
            await test_pool_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    expected_pool_connection.execute.assert_has_calls(
        [mock.call(b'SET', SLOT_ZERO_KEY, 'value')] * 16
    )
    assert "TTL exhausted." in str(e)

@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_with_clusterdown_raises_exception(
    loop,
    nodes,
    cluster_server,
    free_ports,
):
    test_cluster = await create_pool_cluster([nodes[0]], encoding='utf-8', loop=loop)
    expected_pool_connection = FakeConnection(
        free_ports[0],
        loop,
        return_value=ReplyError("CLUSTERDOWN"),
    )

    try:
        with PoolConnectionMock(test_cluster, loop, {free_ports[0]: expected_pool_connection}):
            with pytest.raises(ReplyError):
                await test_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        expected_pool_connection.execute.assert_called_once_with(
            b'SET', SLOT_ZERO_KEY, 'value'
        )
        assert test_cluster._refresh_nodes_asap

        # We should try and re-initialize after this.
        expected_connections_2 = {
            free_ports[0]: FakeConnection(
                free_ports[0],
                loop,
                return_value=ReplyError("ERROR"),
            ),
        }
        with CreateConnectionMock(expected_connections_2):
            with pytest.raises(RedisClusterError) as e:
                await test_cluster.execute("SET", SLOT_ZERO_KEY, "value")

        assert "No cluster info could be loaded from any host" in str(e)
    finally:
        await test_cluster.clear()

@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_with_connection_error_tries_random_node(loop,
                                                                    choice_mock,
                                                                    test_pool_cluster,
                                                                    free_ports):
    # Hack random.choice to always return the host at port 7002.
    def choice_mock_side_effect(seq):
        sorted_seq = sorted(seq, key=lambda k: k.port)
        return sorted_seq[2]

    choice_mock.side_effect = choice_mock_side_effect
    expected_pool_connection = FakeConnection(
        free_ports[0],
        loop,
        return_value=ConnectionError("Uh oh!"),
    )
    expected_pool_connection_2 = FakeConnection(
        free_ports[2],
        loop,
    )

    with PoolConnectionMock(
            test_pool_cluster, loop,
            {
                free_ports[0]: expected_pool_connection,
                free_ports[2]: expected_pool_connection_2,
            }
    ):
        ok = await test_pool_cluster.execute("SET", SLOT_ZERO_KEY, "value")

    assert ok

    expected_pool_connection.execute.assert_called_once_with(
        b"SET", SLOT_ZERO_KEY, "value"
    )
    expected_pool_connection_2.execute.assert_called_once_with(
        b"SET", SLOT_ZERO_KEY, "value"
    )


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_with_reply_error(
        loop, test_pool_cluster, free_ports
):
    expected_connection = FakeConnection(
        free_ports[0], loop, return_value=ReplyError('ERROR')
    )
    with PoolConnectionMock(
            test_pool_cluster, loop, {free_ports[0]: expected_connection}
    ):
        with pytest.raises(ReplyError):
            await test_pool_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_with_protocol_error(
        loop, test_pool_cluster, free_ports
):
    expected_connection = FakeConnection(
        free_ports[0], loop, return_value=ProtocolError('ERROR')
    )
    with PoolConnectionMock(
            test_pool_cluster, loop, {free_ports[0]: expected_connection}
    ):
        with pytest.raises(ProtocolError):
            await test_pool_cluster.execute('SET', SLOT_ZERO_KEY, 'value')

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_command(loop, test_pool_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with PoolConnectionMock(
            test_pool_cluster, loop, {free_ports[0]: expected_connection}
    ):
        ok = await test_pool_cluster.set(SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_with_address(loop, test_pool_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[1], loop)
    with PoolConnectionMock(
            test_pool_cluster, loop, {free_ports[1]: expected_connection}
    ):
        ok = await test_pool_cluster.execute(
            "SET", SLOT_ZERO_KEY, 'value', address=("127.0.0.1", free_ports[1]),
        )

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_pool_execute_many(loop, test_pool_cluster, free_ports):
    expected_connections = {
        port: FakeConnection(port, loop)
        for port in free_ports[:3]
    }

    with PoolConnectionMock(test_pool_cluster, loop, expected_connections):
        ok = await test_pool_cluster.execute('PING', many=True)

    assert ok == [b'OK'] * 3

    for connection in expected_connections.values():
        connection.execute.assert_called_once_with(
            'PING', encoding=mock.ANY
        )


@cluster_test
@pytest.mark.run_loop
async def test_reload_cluster_pool(test_pool_cluster):
    old_pools = list(test_pool_cluster._cluster_pool.values())

    await test_pool_cluster.reload_cluster_pool()

    new_pools = list(test_pool_cluster._cluster_pool.values())
    assert len(new_pools) > 0
    assert {
        id(pool) for pool in old_pools
    }.isdisjoint({id(pool) for pool in new_pools})
    await test_pool_cluster.clear()


@cluster_test
@pytest.mark.run_loop
async def test_get_pool_for_node_new_pool(test_pool_cluster):
    prev_pool_len = len(test_pool_cluster._cluster_pool)
    slave_node = test_pool_cluster.slave_nodes[0]
    await test_pool_cluster.get_pool_for_node(slave_node)
    assert len(test_pool_cluster._cluster_pool) == prev_pool_len + 1


@cluster_test
@pytest.mark.run_loop
async def test_pool_conn_context_given_node(loop, test_pool_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        {free_ports[0]: expected_connection},
    ):
        first_node = next(
            node for node in test_pool_cluster.master_nodes
            if node.port == free_ports[0]
        )
        conn_context = await test_pool_cluster.get_conn_context_for_node(
            first_node
        )
        async with conn_context as conn:
            ok = await conn.set(SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_pool_conn_context_given_slot(loop, test_pool_cluster, free_ports):
    expected_connection = FakeConnection(free_ports[0], loop)
    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        {free_ports[0]: expected_connection},
    ):
        conn_context = await test_pool_cluster.get_conn_context_for_slot(0)
        async with conn_context as conn:
            ok = await conn.set(SLOT_ZERO_KEY, 'value')

    assert ok

    expected_connection.execute.assert_called_once_with(
        b'SET', SLOT_ZERO_KEY, 'value'
    )


@cluster_test
@pytest.mark.run_loop
async def test_pool_conn_context_raises_error(loop, test_pool_cluster):
    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        {},
    ):
        with pytest.raises(RedisClusterError) as e:
            await test_pool_cluster.get_conn_context_for_slot(20000)

    assert "No master available for slot 20000!" in str(e)



@cluster_test
@pytest.mark.run_loop
async def test_keys_command(test_cluster, key_and_slot, zero_slot_key):
    key, _ = key_and_slot

    res = await test_cluster.set(zero_slot_key, 'val')
    assert res

    keys = await test_cluster.keys('*')
    assert sorted(keys) == sorted([key, zero_slot_key])


@cluster_test
@pytest.mark.run_loop
async def test_scan_command(test_cluster, key_and_slot, zero_slot_key):
    key, _ = key_and_slot

    res = await test_cluster.scan()
    assert sorted(res) == sorted([key, zero_slot_key])


@cluster_test
@pytest.mark.run_loop
async def test_get_keys_in_slots(test_cluster, key_and_slot):
    key, slot_for_key = key_and_slot

    res = await test_cluster.cluster_get_keys_in_slots(
        slot_for_key, 10, encoding='utf-8'
    )
    assert res, [key]

    with pytest.raises(TypeError):
        await test_cluster.cluster_get_keys_in_slots(
            'a', 10, encoding='utf-8'
        )


@cluster_test
@pytest.mark.run_loop
async def test_cluster_count_key_in_slot(test_cluster, key_and_slot):
    key, slot_for_key = key_and_slot

    res = await test_cluster.cluster_count_key_in_slots(slot_for_key)
    assert res == 1

    with pytest.raises(TypeError):
        await test_cluster.cluster_count_key_in_slots('a')


@cluster_test
@pytest.mark.run_loop
async def test_cluster_keyslot(test_cluster, key_and_slot):
    key, slot_for_key = key_and_slot

    res = await test_cluster.cluster_keyslot(key)
    assert res == slot_for_key


@cluster_test
@pytest.mark.run_loop
async def test_cluster_info(test_cluster):
    info = await test_cluster.cluster_info()

    assert isinstance(info, dict)
    assert 'cluster_state' in info
    assert 'cluster_slots_assigned' in info
    assert 'cluster_slots_ok' in info
    assert 'cluster_slots_fail' in info
    assert 'cluster_slots_pfail' in info
    assert 'cluster_known_nodes' in info
    assert 'cluster_size' in info
    assert 'cluster_current_epoch' in info
    assert 'cluster_my_epoch' in info
    assert 'cluster_stats_messages_sent' in info
    assert 'cluster_stats_messages_received' in info


@cluster_test
@pytest.mark.run_loop
async def test_cluster_nodes(test_cluster):
    info = list(await test_cluster.cluster_nodes())

    assert isinstance(info, list)

    assert len(info) == NODES_COUNT
    for node in info:
        assert len(node) == 12

        assert 'id' in node
        assert 'flags' in node
        assert isinstance(node['flags'], tuple)

        assert 'master' in node
        assert 'host' in node
        assert 'port' in node
        assert 'nat-port' in node
        assert 'migrations' in node
        assert isinstance(node['migrations'], tuple)

        assert 'ping-sent' in node
        assert 'pong-recv' in node
        assert 'status' in node
        assert 'slots' in node

        assert isinstance(node['slots'], tuple)
        if not node['slots']:
            assert node['master']
            assert 'slave' in node['flags']
        else:
            assert node['master'] is None
            assert 'master' in node['flags']


@cluster_test
@pytest.mark.run_loop
async def test_cluster_save_config(test_cluster):
    res = await test_cluster.cluster_save_config()
    assert res


@cluster_test
@pytest.mark.run_loop
async def test_cluster_slaves(test_cluster):
    my_master = test_cluster.master_nodes[0]

    for node in test_cluster.slave_nodes:
        if node.master == my_master.id:
            slave = node
            break

    res = list(await test_cluster.cluster_slaves(my_master.id))

    slave_info = res[0]
    assert slave.id == slave_info['id']
    assert slave.address == (slave_info['host'], slave_info['port'])
    assert slave.flags[-1] == slave_info['flags'][-1]
    assert slave.master == slave_info['master']
    assert slave.status == slave_info['status']

    with pytest.raises(ReplyError):
        await test_cluster.cluster_slaves(slave.id)


@cluster_test
@pytest.mark.run_loop
async def test_readwrite_readonly(test_cluster):
    slave = test_cluster.slave_nodes[0]

    res = await test_cluster.cluster_readonly(slave.address)
    assert res

    res = await test_cluster.cluster_readwrite(slave.address)
    assert res


@cluster_test
@pytest.mark.run_loop
async def test_add_slots(test_cluster_no_slots_assigned, nodes):
    test_cluster = test_cluster_no_slots_assigned
    for address in nodes:
        slots = await test_cluster.cluster_slots(address=address)
        assert slots == {}

    res = await test_cluster.cluster_add_slots(0, address=nodes[0])
    assert res

    slots = await test_cluster.cluster_slots(address=nodes[0])
    assert slots == {
        (0, 0): nodes[0]
    }

    res = await test_cluster.cluster_add_slots(1, 2, address=nodes[0])
    assert res

    slots = await test_cluster.cluster_slots(address=nodes[0])
    assert slots == {
        (0, 2): nodes[0]
    }

    res = await test_cluster.cluster_add_slots(
        3, 4, 5, address=nodes[0]
    )
    assert res

    slots = await test_cluster.cluster_slots(address=nodes[0])
    assert slots == {
        (0, 5): nodes[0]
    }

    res = await test_cluster.cluster_add_slots(
        7, 7, address=nodes[0]
    )
    assert res

    slots = await test_cluster.cluster_slots(address=nodes[0])
    assert slots == {
        (0, 5): nodes[0],
        (7, 7): nodes[0]
    }

    # Add to different node
    res = await test_cluster.cluster_add_slots(8, address=nodes[1])
    assert res

    async def check_slots():
        slots = await test_cluster.cluster_slots(address=nodes[1])
        assert slots == {
            (0, 5): nodes[0],
            (7, 7): nodes[0],
            (8, 8): nodes[1]
        }

    await _wait_result(check_slots)

    with pytest.raises(ReplyError):
        await test_cluster.cluster_add_slots(0, address=nodes[0])

    with pytest.raises(TypeError):
        await test_cluster.cluster_add_slots('a')


@cluster_test
@pytest.mark.run_loop
async def test_count_failure_reports(test_cluster):
    node = test_cluster.master_nodes[0]
    node_id = node.id
    res = await test_cluster.cluster_count_failure_reports(node_id)
    assert res == 0


@cluster_test
@pytest.mark.run_loop
async def test_del_slots_single(test_cluster, nodes):
    all_slots = ClusterNodesManager.REDIS_CLUSTER_HASH_SLOTS
    masters_count = int(NODES_COUNT / 2)
    slot_boundaries = [
        math.floor(i * all_slots / masters_count)
        for i in range(masters_count + 1)
    ]

    # Only one node will be affected
    ok = await test_cluster.cluster_del_slots(4)
    assert ok

    # Two nodes will be affected
    ok = await test_cluster.cluster_del_slots(slot_boundaries[1], 2, 3)
    assert ok

    node1_slots = await test_cluster.cluster_slots(address=nodes[0])
    expected_node1 = {
        (0, 1): nodes[0],
        (5, slot_boundaries[1] - 1): nodes[0],
        (slot_boundaries[1], slot_boundaries[2] - 1): nodes[1],
        (slot_boundaries[2],  slot_boundaries[3] - 1): nodes[2]
    }
    assert node1_slots == expected_node1

    node2_slots = await test_cluster.cluster_slots(address=nodes[1])
    expected_node2 = {
        (0, slot_boundaries[1] - 1): nodes[0],
        (slot_boundaries[1] + 1, slot_boundaries[2] - 1): nodes[1],
        (slot_boundaries[2], slot_boundaries[3] - 1): nodes[2]
    }
    assert node2_slots == expected_node2

    await test_cluster.cluster_add_slots(2, 3, 4, address=nodes[0])
    await test_cluster.cluster_add_slots(slot_boundaries[1], address=nodes[1])


@cluster_test
@pytest.mark.run_loop(timeout=30)
async def test_del_slots_many(test_cluster, nodes):
    all_slots = ClusterNodesManager.REDIS_CLUSTER_HASH_SLOTS
    masters_count = int(NODES_COUNT / 2)
    slot_boundaries = [
        math.floor(i * all_slots / masters_count)
        for i in range(masters_count + 1)
    ]

    ok = await test_cluster.cluster_del_slots(
        slot_boundaries[1], *range(0, 4), many=True, slaves=True
    )
    assert ok

    async def check_slots():
        slots = await test_cluster.cluster_slots(many=True, slaves=True)
        expected_slots = {
            (4, slot_boundaries[1] - 1): nodes[0],
            (slot_boundaries[1] + 1, slot_boundaries[2] - 1): nodes[1],
            (slot_boundaries[2], slot_boundaries[3] - 1): nodes[2]
        }
        print(slots)
        assert slots == [expected_slots] * 6

    await _wait_result(check_slots)

    await test_cluster.initialize()

    with pytest.raises(RedisClusterError):
        await test_cluster.cluster_del_slots(*range(0, 4))

    with pytest.raises(RedisClusterError):
        await test_cluster.cluster_del_slots(-1)

    with pytest.raises(RedisClusterError):
        await test_cluster.cluster_del_slots(
            ClusterNodesManager.REDIS_CLUSTER_HASH_SLOTS + 1
        )

    with pytest.raises(TypeError):
        await test_cluster.cluster_del_slots('a')

    with pytest.raises(TypeError):
        await test_cluster.cluster_del_slots('a', many=True)


@cluster_test
@pytest.mark.run_loop
async def test_cluster_meet(test_cluster, nodes):
    res = await test_cluster.cluster_meet(*nodes[0])
    assert res

    res = await test_cluster.cluster_meet(*nodes[1], address=nodes[0])
    assert res

    res = await test_cluster.cluster_meet(*nodes[0], address=nodes[0])
    assert res

    res = await test_cluster.cluster_meet(*nodes[0], many=True, slaves=True)
    assert res == [True] * NODES_COUNT


@cluster_test
@pytest.mark.run_loop
async def test_cluster_forget_and_replicate(test_cluster):
    master1 = test_cluster.master_nodes[0]
    master2 = test_cluster.master_nodes[1]
    slave1 = None
    slave2 = None

    for node in test_cluster.slave_nodes:
        if node.master == master1.id:
            slave1 = node
        elif node.master == master2.id:
            slave2 = node

    # Forget on target node
    res = await test_cluster.cluster_forget(
        slave1.id, address=master1.address
    )
    assert res

    # Other nodes still remember
    info = list(await test_cluster.cluster_nodes(address=master2.address))
    assert len(info) >= NODES_COUNT  # We could also catch a handshake row

    with pytest.raises(ReplyError):
        await test_cluster.cluster_forget(
            slave1.id, address=master1.address
        )

    res = await test_cluster.cluster_meet(
        *slave1.address, address=master1.address
    )
    assert res

    res = await test_cluster.cluster_replicate(
        master1.id, address=slave1.address
    )
    assert res

    # Forget on all nodes
    res = await test_cluster.cluster_forget(slave2.id)
    assert res == [True] * (NODES_COUNT - 1)

    async def check_nodes():
        nodes = list(await test_cluster.cluster_nodes())
        assert len(nodes) == NODES_COUNT - 1

    await _wait_result(check_nodes)

    res = await test_cluster.cluster_meet(
        *slave2.address, many=True, slaves=True
    )
    assert res == [True] * NODES_COUNT

    res = await test_cluster.cluster_replicate(
        master2.id, address=slave2.address
    )
    assert res

    info = list(await test_cluster.cluster_nodes(address=slave2.address))
    for node in info:
        if node['id'] == slave2.id:
            assert slave2.master == node['master']
            assert master2.id == node['master']
            break


@cluster_test
@pytest.mark.run_loop
async def test_cluster_set_config_epoch_and_reset(test_cluster):
    nodes = test_cluster.master_nodes
    node = nodes[0]
    other_node = nodes[2]

    res = await test_cluster.cluster_reset(address=node.address)
    assert res

    info = list(await test_cluster.cluster_nodes(address=node.address))
    assert len(info) == 1
    assert info[0]['id'] == node.id

    res = await test_cluster.cluster_reset(address=node.address, hard=True)
    assert res

    info = list(await test_cluster.cluster_nodes(address=node.address))
    assert len(info) == 1
    assert info[0]['id'] != node.id

    res = await test_cluster.cluster_set_config_epoch(
        12, address=node.address
    )
    assert res

    res = await test_cluster.cluster_reset(address=node.address)
    assert res

    # In this case cluster need hard reset
    with pytest.raises(ReplyError):
        await test_cluster.cluster_set_config_epoch(
            1, address=node.address
        )

    with pytest.raises(TypeError):
        await test_cluster.cluster_set_config_epoch(
            'a', address=other_node.address
        )

    with pytest.raises(ReplyError):
        await test_cluster.cluster_set_config_epoch(
            1, address=other_node.address
        )

    res = await test_cluster.cluster_reset()
    assert all(res)


@cluster_test
@pytest.mark.run_loop
async def test_cluster_failover_fail(test_cluster):
    my_master = test_cluster.master_nodes[0]

    with pytest.raises(ReplyError):
        await  test_cluster.cluster_failover(my_master.address)


@cluster_test
@pytest.mark.run_loop
@pytest.mark.parametrize('force', [True, False])
async def test_cluster_failover_ok(force, test_cluster):
    my_master = test_cluster.master_nodes[2]

    async def find_slave_or_reload():
        for node in test_cluster.slave_nodes:
            if node.master == my_master.id:
                return node

        await test_cluster.initialize()
        assert False

    slave = await _wait_result(find_slave_or_reload)

    res = await test_cluster.cluster_failover(slave.address, force)
    assert res

    # Waiting for failover
    while True:
        await asyncio.sleep(0.3)
        await test_cluster.initialize()
        for node in test_cluster.master_nodes:
            if node.id == slave.id:
                return


@cluster_test
@pytest.mark.run_loop
async def test_normal_commands_on_cluster(test_cluster):
    await test_cluster.set('mykey', 123)
    res = await test_cluster.get('mykey')
    assert res == '123'

    await test_cluster.set('otherkey', 456)
    assert set(await test_cluster.keys('*')) == {'mykey', 'otherkey'}


@cluster_test
@pytest.mark.run_loop
async def test_error_on_cluster(test_cluster):
    await test_cluster.set('nohash', 123)
    with pytest.raises(ReplyError):
        await test_cluster.hset('nohash', 1, 2)


@cluster_test
@pytest.mark.run_loop
async def test_multi_key_commands_on_cluster(test_cluster):
    await test_cluster.set('my{key}', 1)
    await test_cluster.set('other{key}', 2)
    await test_cluster.set('otherkey', 3)

    with pytest.raises(ReplyError):
        # these keys map to different slots
        await test_cluster.delete('my{key}', 'otherkey')

    await test_cluster.delete('my{key}', 'other{key}')

    assert await test_cluster.get('my{key}') is None
    assert await test_cluster.get('other{key}') is None


@cluster_test
@pytest.mark.run_loop
async def test_pipeline_executes_on_multiple_keys(loop, test_cluster):
    pipeline = test_cluster.pipeline()
    fut1 = pipeline.set("{test}key", "testval")
    fut2 = pipeline.set("{test}key2", "testval2")
    fut3 = pipeline.hset("{test}key3", "testfield3", "testval3")

    result = await pipeline.execute()
    result_check = await asyncio.gather(fut1, fut2, fut3, loop=loop)
    assert result == result_check

    assert result == [True, True, True]

    assert (await test_cluster.get("{test}key")) == "testval"
    assert (await test_cluster.get("{test}key2")) == "testval2"
    assert (await test_cluster.hget("{test}key3", "testfield3")) == "testval3"


@cluster_test
@pytest.mark.run_loop
async def test_cant_execute_pipeline_twice(loop, test_cluster):
    pipeline = test_cluster.pipeline()
    fut1 = pipeline.set("{test}key", "testval")
    fut2 = pipeline.set("{test}key2", "testval2")
    fut3 = pipeline.hset("{test}key3", "testfield3", "testval3")

    result = await pipeline.execute()
    result_check = await asyncio.gather(fut1, fut2, fut3, loop=loop)
    assert result == result_check

    assert result == [True, True, True]

    with pytest.raises(AssertionError) as e:
        await pipeline.execute()

    assert "Pipeline already executed. Create new one." in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pipeline_raises_and_cancels_futures_with_diff_nodes(loop,
                                                                   test_cluster):
    pipeline = test_cluster.pipeline()
    fut1 = pipeline.set("{test}key", "testval")
    with pytest.raises(InvalidPipelineOperation) as e:
        pipeline.set("key2", "testval2")

    try:
        await asyncio.gather(fut1, loop=loop)
    except asyncio.CancelledError:
        pass
    assert fut1.cancelled()
    for fut, _, _, _ in pipeline._pipeline:
        assert fut.cancelled()

    assert pipeline._done

    assert "All keys in pipeline must point to same node!" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pipeline_raises_errors_from_individual_operations(loop,
                                                                 test_cluster,
                                                                 free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("UH OH!")
    )
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)
    with CreateConnectionMock({free_ports[0]: expected_connection}):
        pipeline = test_cluster.pipeline()
        fut1 = pipeline.set(SLOT_ZERO_KEY, "testval")
        fut2 = pipeline.set(second_key, "testval2")
        # This is a generic pipeline error.
        with pytest.raises(PipelineError):
            await pipeline.execute()

        # This is the specific error from the connection.
        with pytest.raises(ReplyError) as e:
            await asyncio.gather(fut1, fut2, loop=loop)

    expected_connection.execute.assert_has_calls([
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    assert "UH OH!" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pipeline_retries_on_moved_error(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
    )
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)
    with CreateConnectionMock({
        free_ports[0]: expected_connection,
        free_ports[2]: expected_connection_2,
    }):
        pipeline = test_cluster.pipeline()
        fut1 = pipeline.set(SLOT_ZERO_KEY, "testval")
        fut2 = pipeline.set(second_key, "testval2")
        res = await pipeline.execute()

        # The future results returned here will unfortunately contain the
        # original exceptions.
        res_check = await asyncio.gather(
            fut1,
            fut2,
            loop=loop,
            return_exceptions=True,
        )

    expected_connection.execute.assert_has_calls([
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    expected_connection_2.execute.assert_has_calls([
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    assert isinstance(res_check[0], ReplyError)
    assert isinstance(res_check[1], ReplyError)
    assert res == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_pipeline_retries_on_ask_error(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("ASK 0 127.0.0.1:{}".format(free_ports[2])),
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
    )
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)
    with CreateConnectionMock({
        free_ports[0]: expected_connection,
        free_ports[2]: expected_connection_2,
    }):
        pipeline = test_cluster.pipeline()
        fut1 = pipeline.set(SLOT_ZERO_KEY, "testval")
        fut2 = pipeline.set(second_key, "testval2")
        res = await pipeline.execute()

        # The future results returned here will unfortunately contain the
        # original exceptions.
        res_check = await asyncio.gather(
            fut1,
            fut2,
            loop=loop,
            return_exceptions=True,
        )

    expected_connection.execute.assert_has_calls([
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    expected_connection_2.execute.assert_has_calls([
        mock.call(b"ASKING"),
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"ASKING"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    assert isinstance(res_check[0], ReplyError)
    assert isinstance(res_check[1], ReplyError)
    assert res == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_pool_pipeline_executes_on_multiple_keys(loop, test_pool_cluster):
    pipeline = test_pool_cluster.pipeline()
    fut1 = pipeline.set("{test}key", "testval")
    fut2 = pipeline.set("{test}key2", "testval2")
    fut3 = pipeline.hset("{test}key3", "testfield3", "testval3")

    result = await pipeline.execute()
    result_check = await asyncio.gather(fut1, fut2, fut3, loop=loop)
    assert result == result_check

    assert result == [True, True, True]

    assert (await test_pool_cluster.get("{test}key")) == "testval"
    assert (await test_pool_cluster.get("{test}key2")) == "testval2"
    assert (await test_pool_cluster.hget("{test}key3", "testfield3")) == \
        "testval3"


@cluster_test
@pytest.mark.run_loop
async def test_pool_cant_execute_pipeline_twice(loop, test_pool_cluster):
    pipeline = test_pool_cluster.pipeline()
    fut1 = pipeline.set("{test}key", "testval")
    fut2 = pipeline.set("{test}key2", "testval2")
    fut3 = pipeline.hset("{test}key3", "testfield3", "testval3")

    result = await pipeline.execute()
    result_check = await asyncio.gather(fut1, fut2, fut3, loop=loop)
    assert result == result_check

    assert result == [True, True, True]

    with pytest.raises(AssertionError) as e:
        await pipeline.execute()

    assert "Pipeline already executed. Create new one." in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pool_pipeline_raises_and_cancels_futures_with_diff_nodes(
    loop,
    test_pool_cluster
):
    pipeline = test_pool_cluster.pipeline()
    fut1 = pipeline.set("{test}key", "testval")
    with pytest.raises(InvalidPipelineOperation) as e:
        pipeline.set("key2", "testval2")

    try:
        await asyncio.gather(fut1, loop=loop)
    except asyncio.CancelledError:
        pass
    assert fut1.cancelled()
    for fut, _, _, _ in pipeline._pipeline:
        assert fut.cancelled()

    assert pipeline._done

    assert "All keys in pipeline must point to same node!" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pool_pipeline_raises_errors_from_individual_operations(
    loop,
    test_pool_cluster,
    free_ports,
):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("UH OH!")
    )
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)
    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        {free_ports[0]: expected_connection},
    ):
        pipeline = test_pool_cluster.pipeline()
        fut1 = pipeline.set(SLOT_ZERO_KEY, "testval")
        fut2 = pipeline.set(second_key, "testval2")
        # This is a generic pipeline error.
        with pytest.raises(PipelineError):
            await pipeline.execute()

        # This is the specific error from the connection.
        with pytest.raises(ReplyError) as e:
            await asyncio.gather(fut1, fut2, loop=loop)

    expected_connection.execute.assert_has_calls([
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    assert "UH OH!" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pool_pipeline_retries_on_moved_error(loop,
                                                    test_pool_cluster,
                                                    free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
    )
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)
    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        {
            free_ports[0]: expected_connection,
            free_ports[2]: expected_connection_2,
        },
    ):
        pipeline = test_pool_cluster.pipeline()
        fut1 = pipeline.set(SLOT_ZERO_KEY, "testval")
        fut2 = pipeline.set(second_key, "testval2")
        res = await pipeline.execute()

        # The future results returned here will unfortunately contain the
        # original exceptions.
        res_check = await asyncio.gather(
            fut1,
            fut2,
            loop=loop,
            return_exceptions=True,
        )

    expected_connection.execute.assert_has_calls([
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    expected_connection_2.execute.assert_has_calls([
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    assert isinstance(res_check[0], ReplyError)
    assert isinstance(res_check[1], ReplyError)
    assert res == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_pool_pipeline_retries_on_ask_error(loop,
                                                  test_pool_cluster,
                                                  free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("ASK 0 127.0.0.1:{}".format(free_ports[2])),
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
    )
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)
    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        {
            free_ports[0]: expected_connection,
            free_ports[2]: expected_connection_2,
        },
    ):
        pipeline = test_pool_cluster.pipeline()
        fut1 = pipeline.set(SLOT_ZERO_KEY, "testval")
        fut2 = pipeline.set(second_key, "testval2")
        res = await pipeline.execute()

        # The future results returned here will unfortunately contain the
        # original exceptions.
        res_check = await asyncio.gather(
            fut1,
            fut2,
            loop=loop,
            return_exceptions=True,
        )

    expected_connection.execute.assert_has_calls([
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    expected_connection_2.execute.assert_has_calls([
        mock.call(b"ASKING"),
        mock.call(b"SET", SLOT_ZERO_KEY, "testval"),
        mock.call(b"ASKING"),
        mock.call(b"SET", second_key, "testval2"),
    ])
    assert isinstance(res_check[0], ReplyError)
    assert isinstance(res_check[1], ReplyError)
    assert res == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_transaction_with_keys_same_slot(loop,
                                               test_cluster,
                                               free_ports):

    async def _coro(cluster, transaction):
        transaction.set("{key}1", "1")
        transaction.set("{key}2", "2")

    # Don't worry about watching for now.
    result = await test_cluster.transaction(_coro)
    assert result == [True, True]

    assert (await test_cluster.get("{key}1")) == "1"
    assert (await test_cluster.get("{key}2")) == "2"


@cluster_test
@pytest.mark.run_loop
async def test_transaction_with_keys_different_slot_errors(loop,
                                                           test_cluster,
                                                           free_ports):
    async def _coro(cluster, transaction):
        transaction.set("{key}1", "1")
        transaction.set("{key1}2", "2")

    # Don't worry about watching for now.
    with pytest.raises(InvalidPipelineOperation):
        await test_cluster.transaction(_coro)

    assert (await test_cluster.get("{key}1")) is None
    assert (await test_cluster.get("{key1}2")) is None


@cluster_test
@pytest.mark.run_loop
async def test_transaction_with_keys_different_slot_and_watch_errors(
    loop,
    test_cluster,
    free_ports,
):
    async def _coro(cluster, transaction):
        transaction.set("{key1}2", "2")

    with pytest.raises(InvalidPipelineOperation):
        await test_cluster.transaction(_coro, "{key}1")

    assert (await test_cluster.get("{key}2")) is None


@cluster_test
@pytest.mark.run_loop
async def test_transaction_properly_calls_watch(loop, test_cluster, free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            [b"OK", b"OK"],  # EXECUTE
        ],
    )

    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with CreateConnectionMock({free_ports[0]: expected_connection}):
        result = await test_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert result == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_transaction_handles_moved_in_watch(loop,
                                                  test_cluster,
                                                  free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            [b"OK", b"OK"],  # EXECUTE
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
        free_ports[2]: expected_connection_2,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with CreateConnectionMock(expected_connections):
        result = await test_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_called_once_with(
        "WATCH",
        watched_key,
    )
    expected_connection_2.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert result == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_transaction_handles_moved_no_watched_keys(loop,
                                                         test_cluster,
                                                         free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # MULTI,
            ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),  # CMD 1
            ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),  # CMD 2
            ReplyError("EXECABORT"),
        ],
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
        side_effect=[
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            [b"OK", b"OK"],  # EXECUTE
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
        free_ports[2]: expected_connection_2,
    }
    first_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(first_key, "1")
        trans.set(second_key, "2")

    with CreateConnectionMock(expected_connections):
        result = await test_cluster.transaction(_coro)

    expected_connection.execute.assert_has_calls([
        mock.call("MULTI"),
        mock.call(b"SET", first_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    expected_connection_2.execute.assert_has_calls([
        mock.call("MULTI"),
        mock.call(b"SET", first_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert result == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_transaction_handles_moved_in_transaction(loop,
                                                        test_cluster,
                                                        free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI,
            ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),  # CMD 1
            ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),  # CMD 2
            ReplyError("EXECABORT"),
        ],
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            [b"OK", b"OK"],  # EXECUTE
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
        free_ports[2]: expected_connection_2,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with CreateConnectionMock(expected_connections):
        result = await test_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    expected_connection_2.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert result == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_transaction_raises_unknown_watch_reply_error(loop,
                                                            test_cluster,
                                                            free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("UH OH"),
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with CreateConnectionMock(expected_connections):
        with pytest.raises(ReplyError) as e:
            await test_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_called_once_with(
        "WATCH", watched_key,
    )
    assert "UH OH" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_transaction_raises_unknown_watch_error(loop,
                                                      test_cluster,
                                                      free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ConnectionError("UH OH"),
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with CreateConnectionMock(expected_connections):
        with pytest.raises(ConnectionError) as e:
            await test_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_called_once_with(
        "WATCH", watched_key,
    )
    assert "UH OH" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_transaction_raises_error_from_transaction(loop,
                                                         test_cluster,
                                                         free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            ReplyError("UH OH"),  # EXEC
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with CreateConnectionMock(expected_connections):
        with pytest.raises(MultiExecError) as e:
            await test_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert "ReplyError('UH OH',)" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_transaction_handles_exception_in_coro(loop,
                                                     test_cluster,
                                                     free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"WRONG_VALUE", # GET
            b"OK",  # UNWATCH
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    async def _coro(cluster, transaction):
        val = await cluster.get(watched_key)
        if val != "RIGHT_VALUE":
            raise Exception("Got the wrong value!")
        transaction.set(watched_key, "123")

    with CreateConnectionMock(expected_connections):
        with pytest.raises(Exception) as e:
            await test_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call(b"GET", watched_key, encoding=_NOTSET),
        mock.call(b"UNWATCH"),
    ])


@cluster_test
@pytest.mark.run_loop
async def test_transaction_handles_False_in_coro(loop,
                                                 test_cluster,
                                                 free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"WRONG_VALUE", # GET
            b"OK",  # UNWATCH
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    async def _coro(cluster, transaction):
        val = await cluster.get(watched_key)
        if val != "RIGHT_VALUE":
            return False
        transaction.set(watched_key, "123")

    with CreateConnectionMock(expected_connections):
        result = await test_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call(b"GET", watched_key, encoding=_NOTSET),
        mock.call(b"UNWATCH"),
    ])
    assert result == []


@cluster_test
@pytest.mark.run_loop
async def test_transaction_handles_False_in_coro_no_watched_keys(loop,
                                                                 test_cluster,
                                                                 free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"WRONG_VALUE", # GET
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    key = "{{{}}}1".format(SLOT_ZERO_KEY)
    async def _coro(cluster, transaction):
        val = await cluster.get(key)
        if val != "RIGHT_VALUE":
            return False
        transaction.set(key, "123")

    with CreateConnectionMock(expected_connections):
        result = await test_cluster.transaction(_coro)

    expected_connection.execute.assert_called_once_with(
        b"GET", key, encoding=_NOTSET
    )
    assert result == []


@cluster_test
@pytest.mark.run_loop
async def test_transaction_handles_empty_pipeline_no_watch(loop,
                                                           test_cluster,
                                                           free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"WRONG_VALUE", # GET
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    key = "{{{}}}1".format(SLOT_ZERO_KEY)
    async def _coro(cluster, transaction):
        val = await cluster.get(key)

    with CreateConnectionMock(expected_connections):
        result = await test_cluster.transaction(_coro)

    expected_connection.execute.assert_called_once_with(
        b"GET", key, encoding=_NOTSET
    )
    assert result == []


@cluster_test
@pytest.mark.run_loop
async def test_transaction_handles_empty_pipeline_with_watch(loop,
                                                             test_cluster,
                                                             free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",          # WATCH
            b"WRONG_VALUE", # GET
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    key = "{{{}}}1".format(SLOT_ZERO_KEY)
    async def _coro(cluster, transaction):
        val = await cluster.get(key)

    with CreateConnectionMock(expected_connections):
        result = await test_cluster.transaction(_coro, key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", key),
        mock.call(b"GET", key, encoding=_NOTSET),
    ])
    assert result == []


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_with_keys_same_slot(loop,
                                                    test_pool_cluster,
                                                    free_ports):

    async def _coro(cluster, transaction):
        transaction.set("{key}1", "1")
        transaction.set("{key}2", "2")

    # Don't worry about watching for now.
    result = await test_pool_cluster.transaction(_coro)
    assert result == [True, True]

    assert (await test_pool_cluster.get("{key}1")) == "1"
    assert (await test_pool_cluster.get("{key}2")) == "2"


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_with_keys_different_slot_errors(
    loop,
    test_pool_cluster,
    free_ports
):
    async def _coro(cluster, transaction):
        transaction.set("{key}1", "1")
        transaction.set("{key1}2", "2")

    # Don't worry about watching for now.
    with pytest.raises(InvalidPipelineOperation):
        await test_pool_cluster.transaction(_coro)

    assert (await test_pool_cluster.get("{key}1")) is None
    assert (await test_pool_cluster.get("{key1}2")) is None


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_with_keys_different_slot_and_watch_errors(
    loop,
    test_pool_cluster,
    free_ports,
):
    async def _coro(cluster, transaction):
        transaction.set("{key1}2", "2")

    with pytest.raises(InvalidPipelineOperation):
        await test_pool_cluster.transaction(_coro, "{key}1")

    assert (await test_pool_cluster.get("{key}2")) is None


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_properly_calls_watch(
    loop,
    test_pool_cluster,
    free_ports,
):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            [b"OK", b"OK"],  # EXECUTE
        ],
    )

    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        {free_ports[0]: expected_connection},
    ):
        result = await test_pool_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert result == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_handles_moved_in_watch(loop,
                                                       test_pool_cluster,
                                                       free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            [b"OK", b"OK"],  # EXECUTE
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
        free_ports[2]: expected_connection_2,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        result = await test_pool_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_called_once_with(
        "WATCH",
        watched_key,
    )
    expected_connection_2.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert result == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_handles_moved_no_watched_keys(loop,
                                                              test_pool_cluster,
                                                              free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # MULTI,
            ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),  # CMD 1
            ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),  # CMD 2
            ReplyError("EXECABORT"),
        ],
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
        side_effect=[
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            [b"OK", b"OK"],  # EXECUTE
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
        free_ports[2]: expected_connection_2,
    }
    first_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(first_key, "1")
        trans.set(second_key, "2")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        result = await test_pool_cluster.transaction(_coro)

    expected_connection.execute.assert_has_calls([
        mock.call("MULTI"),
        mock.call(b"SET", first_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    expected_connection_2.execute.assert_has_calls([
        mock.call("MULTI"),
        mock.call(b"SET", first_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert result == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_handles_moved_in_transaction(loop,
                                                             test_pool_cluster,
                                                             free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI,
            ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),  # CMD 1
            ReplyError("MOVED 0 127.0.0.1:{}".format(free_ports[2])),  # CMD 2
            ReplyError("EXECABORT"),
        ],
    )
    expected_connection_2 = FakeConnection(
        port=free_ports[2],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            [b"OK", b"OK"],  # EXECUTE
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
        free_ports[2]: expected_connection_2,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        result = await test_pool_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    expected_connection_2.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert result == [True, True]


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_raises_unknown_watch_reply_error(
    loop,
    test_pool_cluster,
    free_ports,
):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ReplyError("UH OH"),
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        with pytest.raises(ReplyError) as e:
            await test_pool_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_called_once_with(
        "WATCH", watched_key,
    )
    assert "UH OH" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_raises_unknown_watch_error(loop,
                                                           test_pool_cluster,
                                                           free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        return_value=ConnectionError("UH OH"),
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        with pytest.raises(ConnectionError) as e:
            await test_pool_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_called_once_with(
        "WATCH", watched_key,
    )
    assert "UH OH" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_raises_error_from_transaction(loop,
                                                              test_pool_cluster,
                                                              free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"OK",  # MULTI
            b"QUEUED",  # COMMAND 1
            b"QUEUED",  # COMMAND 2
            ReplyError("UH OH"),  # EXEC
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    second_key = "{{{}}}2".format(SLOT_ZERO_KEY)

    async def _coro(cluster, trans):
        trans.set(watched_key, "1")
        trans.set(second_key, "2")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        with pytest.raises(MultiExecError) as e:
            await test_pool_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call("MULTI"),
        mock.call(b"SET", watched_key, "1"),
        mock.call(b"SET", second_key, "2"),
        mock.call("EXEC"),
    ])
    assert "ReplyError('UH OH',)" in str(e)


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_handles_exception_in_coro(loop,
                                                          test_pool_cluster,
                                                          free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"WRONG_VALUE", # GET
            b"OK",  # UNWATCH
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    async def _coro(cluster, transaction):
        val = await cluster.get(watched_key)
        if val != "RIGHT_VALUE":
            raise Exception("Got the wrong value!")
        transaction.set(watched_key, "123")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        with pytest.raises(Exception) as e:
            await test_pool_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call(b"GET", watched_key, encoding=_NOTSET),
        mock.call(b"UNWATCH"),
    ])


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_handles_False_in_coro(loop,
                                                      test_pool_cluster,
                                                      free_ports):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"OK",  # WATCH
            b"WRONG_VALUE", # GET
            b"OK",  # UNWATCH
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    watched_key = "{{{}}}1".format(SLOT_ZERO_KEY)
    async def _coro(cluster, transaction):
        val = await cluster.get(watched_key)
        if val != "RIGHT_VALUE":
            return False
        transaction.set(watched_key, "123")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        result = await test_pool_cluster.transaction(_coro, watched_key)

    expected_connection.execute.assert_has_calls([
        mock.call("WATCH", watched_key),
        mock.call(b"GET", watched_key, encoding=_NOTSET),
        mock.call(b"UNWATCH"),
    ])
    assert result == []


@cluster_test
@pytest.mark.run_loop
async def test_pool_transaction_handles_False_in_coro_no_watched_keys(
    loop,
    test_pool_cluster,
    free_ports
):
    expected_connection = FakeConnection(
        port=free_ports[0],
        loop=loop,
        side_effect=[
            b"WRONG_VALUE", # GET
        ],
    )
    expected_connections = {
        free_ports[0]: expected_connection,
    }
    key = "{{{}}}1".format(SLOT_ZERO_KEY)
    async def _coro(cluster, transaction):
        val = await cluster.get(key)
        if val != "RIGHT_VALUE":
            return False
        transaction.set(key, "123")

    with PoolConnectionMock(
        test_pool_cluster,
        loop,
        expected_connections,
    ):
        result = await test_pool_cluster.transaction(_coro)

    expected_connection.execute.assert_called_once_with(
        b"GET", key, encoding=_NOTSET
    )
    assert result == []
