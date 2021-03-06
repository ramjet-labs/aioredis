import asyncio
import random
from functools import partial

from aioredis.abc import AbcPool
from aioredis.commands import Redis, create_redis, create_redis_pool
from aioredis.errors import ProtocolError, RedisClusterError, ReplyError
from aioredis.log import logger
from aioredis.util import cached_property, decode, encode_str

from .base import RedisClusterBase
from .crc import crc16
from .transaction import ClusterTransactionsMixin
from .util import CONNECTION_ERRORS, parse_cluster_response_error

__all__ = (
    'create_pool_cluster',
    'RedisPoolCluster',
    'create_cluster',
    'RedisCluster',
)


class ClusterNode:
    def __init__(
            self, number, id, host, port, flags, master, status, slots,
            **kwargs
    ):
        self.id = id
        self.host = host
        self.port = port
        self.flags = flags
        self.master = master
        self.status = status
        self.slots = slots
        self.number = number

    def __repr__(self):
        return r'Address: {!r}. Master: {!r}. Slave: {!r}. Alive: {!r}'.format(
            self.address, self.is_master, self.is_slave, self.is_alive)

    @cached_property
    def is_master(self):
        return 'master' in self.flags

    @cached_property
    def is_slave(self):
        return 'slave' in self.flags

    @cached_property
    def address(self):
        return self.host, self.port

    @cached_property
    def is_alive(self):
        return ('fail' not in self.flags and
                'fail?' not in self.flags and
                self.status == 'connected')

    def in_range(self, value):
        if not self.slots:
            return False

        if value < self.slots[0][0]:
            return False
        if value > self.slots[-1][-1]:
            return False
        return any(rng[0] <= value <= rng[1] for rng in self.slots)


class ClusterNodesManager:

    REDIS_CLUSTER_HASH_SLOTS = 16384

    def __init__(self, nodes):
        # self.slots will be a dict mapping slot_id -> list of nodes, where the
        # first node is always the master.
        self.slots = {}
        nodes = list(nodes)
        masters_slots = {node.id: node.slots for node in nodes}
        for node in nodes:
            if node.is_slave:
                node.slots = masters_slots[node.master]
                for slot_rng in node.slots:
                    for slot in range(slot_rng[0], slot_rng[1] + 1):
                        if slot in self.slots:
                            self.slots[slot].append(node)
                        else:
                            self.slots[slot] = [node]
            elif node.is_master:
                for slot_rng in node.slots:
                    for slot in range(slot_rng[0], slot_rng[1] + 1):
                        if slot in self.slots:
                            # Prefer the node that is alive.
                            if node.is_alive:
                                self.slots[slot].insert(0, node)
                            else:
                                self.slots[slot].insert(1, node)
                        else:
                            self.slots[slot] = [node]
        self.nodes = nodes

    def __repr__(self):
        return r' == '.join(repr(node) for node in self.nodes)

    def __str__(self):
        return '\n'.join(repr(node) for node in self.nodes)

    @classmethod
    def parse_info(cls, info):
        for index, node_data in enumerate(info):
            yield ClusterNode(index, **node_data)

    @classmethod
    def create(cls, data):
        nodes = cls.parse_info(data)
        return cls(nodes)

    @staticmethod
    def key_slot(key, bucket=REDIS_CLUSTER_HASH_SLOTS):
        """Calculate key slot for a given key.

        :param key - str|bytes
        :param bucket - int
        """
        k = encode_str(key)
        start = k.find(b'{')
        if start > -1:
            end = k.find(b'}', start + 1)
            if end > -1 and end != start + 1:
                k = k[start + 1:end]
        return crc16(k) % bucket

    @cached_property
    def alive_nodes(self):
        return [node for node in self.nodes if node.is_alive]

    @cached_property
    def nodes_count(self):
        return len(self.alive_nodes)

    @cached_property
    def masters_count(self):
        return len(self.masters)

    @cached_property
    def slaves_count(self):
        return len(self.slaves)

    @cached_property
    def masters(self):
        return [node for node in self.alive_nodes if node.is_master]

    @cached_property
    def slaves(self):
        return [node for node in self.alive_nodes if node.is_slave]

    @cached_property
    def all_slots_covered(self):
        covered_slots_number = sum(
            end - start + 1
            for master in self.masters for start, end in master.slots
        )
        return covered_slots_number >= self.REDIS_CLUSTER_HASH_SLOTS

    def get_node_by_slot(self, slot):
        if slot not in self.slots:
            return None
        node = self.slots[slot][0]
        if not node.is_master:
            return None
        return node

    def get_node_by_id(self, node_id):
        for node in self.nodes:
            if node_id == node.id:
                return node
        else:
            return None

    def get_node_by_address(self, address):
        for node in self.nodes:
            if address == node.address:
                return node
        else:
            return None

    def get_random_node(self):
        return random.choice(self.alive_nodes)

    def get_random_master_node(self, ignore_address=None):
        if ignore_address:
            masters = [node for node in self.masters
                       if node.address != ignore_address]
        else:
            masters = self.masters
        return random.choice(masters)

    def get_random_slave_node(self):
        return random.choice(self.slaves)

    def determine_slot(self, *keys):
        if any(key is None for key in keys):
            raise TypeError('key must not be None')
        if len(keys) == 1:
            return self.key_slot(keys[0])
        else:
            slots = {self.key_slot(key) for key in keys}
            if len(slots) != 1:
                raise RedisClusterError(
                    'all keys must map to the same key slot')
            return slots.pop()


async def create_pool_cluster(
        nodes, *, db=0, password=None, encoding=None,
        minsize=10, maxsize=10, commands_factory=Redis,
        timeout=None):
    """
    Create Redis Pool Cluster.

    :param nodes = [(address1, port1), (address2, port2), ...]
    :param db - int
    :param password: str
    :param encoding: str
    :param minsize: int
    :param maxsize: int
    :param commands_factory: obj
    :param timeout: int
    :return RedisPoolCluster instance.
    """
    if not nodes or not isinstance(nodes, (tuple, list)):
        raise RedisClusterError(
            'Cluster nodes is not set properly. {0}'.
            format(create_pool_cluster.__doc__))

    cluster = RedisPoolCluster(
        nodes, db, password, encoding=encoding, minsize=minsize,
        maxsize=maxsize, commands_factory=commands_factory,
        timeout=timeout)
    await cluster.initialize()
    return cluster


async def create_cluster(
        nodes, *, db=0, password=None, encoding=None,
        commands_factory=Redis):
    """
    Create Redis Pool Cluster.

    :param nodes = [(address1, port1), (address2, port2), ...]
    :param db - int
    :param password: str
    :param encoding: str
    :param commands_factory: obj
    :return RedisPoolCluster instance.
    """
    if not nodes or not isinstance(nodes, (tuple, list)):
        raise RedisClusterError(
            'Cluster nodes is not set properly. {0}'.
            format(create_cluster.__doc__))

    cluster = RedisCluster(
        nodes, db, password, encoding=encoding,
        commands_factory=commands_factory)
    await cluster.initialize()
    return cluster


class RedisCluster(RedisClusterBase, ClusterTransactionsMixin):
    """Redis cluster."""

    MAX_MOVED_COUNT = 10
    REQUEST_TTL = 16

    def __init__(self, nodes, db=0, password=None, encoding=None,
                 *, commands_factory):
        self._nodes = nodes
        self._db = db
        self._password = password
        self._encoding = encoding
        self._factory = commands_factory
        self._moved_count = 0
        self._cluster_manager = None
        self._initalize_lock = asyncio.Lock()
        self._refresh_nodes_asap = False

    def _is_eval_command(self, command):
        if isinstance(command, bytes):
            command = command.decode('utf-8')
        return command.lower() in ['eval', 'evalsha']

    def _is_multi_key_command(self, command):
        if isinstance(command, bytes):
            command = command.decode('utf-8')
        return command.lower() in ['watch', 'del']

    def get_slot(self, command, *args, **kwargs):
        if self._is_eval_command(command):
            keys = kwargs.get('keys', [])
            if not isinstance(keys, (list, tuple)):
                raise TypeError('keys must be given as list or tuple')
        elif self._is_multi_key_command(command):
            keys = args
        else:
            keys = args[:1]

        if keys:
            return self._cluster_manager.determine_slot(*keys)
        return None

    def get_node(self, command, *args, **kwargs):
        slot = self.get_slot(command, *args, **kwargs)
        if slot is not None:
            node = self._cluster_manager.get_node_by_slot(slot)
            if node is not None:
                return node

        return self._cluster_manager.get_random_master_node()

    def node_count(self):
        return self._cluster_manager.nodes_count

    def masters_count(self):
        return self._cluster_manager.masters_count

    def slave_count(self):
        return self._cluster_manager.slaves_count

    def _get_nodes_entities(self, slaves=False):
        slave_nodes = []
        if slaves:
            slave_nodes = [node.address for node in self.slave_nodes]
        return [node.address for node in self.master_nodes] + slave_nodes

    @property
    def master_nodes(self):
        return self._cluster_manager.masters

    @property
    def slave_nodes(self):
        return self._cluster_manager.slaves

    async def increment_moved_count(self):
        self._moved_count += 1
        if self._moved_count >= self.MAX_MOVED_COUNT:
            async with self._initalize_lock:
                if self._moved_count >= self.MAX_MOVED_COUNT:
                    await self.initialize()

    async def initialize_asap(self):
        self._refresh_nodes_asap = True
        async with self._initalize_lock:
            if self._refresh_nodes_asap:
                await self.initialize()

    async def _get_raw_cluster_info_from_node(self, node):
        conn = await create_redis(
            node,
            db=self._db,
            password=self._password,
            encoding='utf-8',
            commands_factory=self._factory,
        )

        try:
            nodes_resp = await conn.cluster_nodes()
            return nodes_resp
        finally:
            conn.close()
            await conn.wait_closed()

    async def fetch_cluster_info(self):
        logger.debug('Loading cluster info from %s...', self._nodes)
        tasks = [
            asyncio.ensure_future(
                self._get_raw_cluster_info_from_node(node)
            ) for node in self._nodes
        ]
        try:
            for task in asyncio.as_completed(tasks):
                try:
                    nodes_raw_response = list(await task)
                    self._cluster_manager = ClusterNodesManager.create(
                        nodes_raw_response
                    )
                    logger.debug('Cluster info loaded successfully: %s',
                                  nodes_raw_response)
                    return
                except (ReplyError, ProtocolError,
                        ConnectionError, OSError) as exc:
                    logger.warning(
                        "Loading cluster info from a node failed with {}"
                        .format(repr(exc))
                    )
        finally:
            for task in tasks:
                task.cancel()
            # Wait until all tasks have closed their connection
            await asyncio.gather(
                *tasks, return_exceptions=True)

        raise RedisClusterError(
            "No cluster info could be loaded from any host")

    async def initialize(self):
        logger.debug('Initializing cluster...')
        self._moved_count = 0
        self._refresh_nodes_asap = False
        await self.fetch_cluster_info()
        logger.debug('Initialized cluster.\n%s', self._cluster_manager)

    async def clear(self):
        pass  # All connections are created on demand and destroyed afterwards.

    @property
    def all_slots_covered(self):
        return self._cluster_manager.all_slots_covered

    async def create_connection(self, address):
        conn = await create_redis(
            address,
            db=self._db,
            encoding=self._encoding,
            password=self._password,
            commands_factory=self._factory,
        )
        return conn

    async def get_conn_context_for_address(self, address):
        node = self._cluster_manager.get_node_by_address(
            address
        )
        if node:
            redis = await self.create_connection(node.address)
            return ClusterConnectionContext(redis)
        await self.initialize_asap()
        node = self._cluster_manager.get_node_by_address(
            address
        )
        if node is None:
            raise RedisClusterError(
                "Could not find node with address {}".format(address)
            )
        redis = await self.create_connection(node.address)
        return ClusterConnectionContext(redis)

    async def get_conn_context_for_node(self, node):
        redis = await self.create_connection(node.address)
        return ClusterConnectionContext(redis)

    async def get_conn_context_for_slot(self, slot):
        node = self._cluster_manager.get_node_by_slot(slot)
        if not node:
            raise RedisClusterError(
                "No master available for slot {}!".format(slot)
            )
        redis = await self.create_connection(node.address)
        return ClusterConnectionContext(redis)

    async def update_node_for_slot(self, address, slot):
        """
        Updates the node responsible for a given slot. If no known node exists
        with the given address, then an initialization is queued to get an
        up-to-date topology.
        """
        node = self._cluster_manager.get_node_by_address(
            address
        )
        if node:
            self._cluster_manager.slots[slot][0] = node
        else:
            await self.initialize_asap()
            node = self._cluster_manager.get_node_by_address(
                address
            )
            if node is None:
                raise RedisClusterError(
                    "Could not find node with address {}".format(address)
                )
            self._cluster_manager.slots[slot][0] = node

    async def _execute_node(self, address, command, *args, asking=False, **kwargs):
        """Execute redis command and returns Future waiting for the answer.

        :param command str
        :param pool obj
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """
        cmd = decode(command, 'utf-8').lower()
        try_random_node = False
        ttl = int(self.REQUEST_TTL)
        connections = {}

        try:
            while ttl > 0:
                ttl -= 1
                try:
                    if try_random_node:
                        node = self._cluster_manager.get_random_master_node()
                        if node.address in connections:
                            conn = connections[node.address]
                        else:
                            conn = await self.create_connection(node.address)
                            connections[node.address] = conn
                        try_random_node = False
                    else:
                        if address in connections:
                            conn = connections[address]
                        else:
                            conn = await self.create_connection(address)
                            connections[address] = conn

                    if asking:
                        await conn.execute(b"ASKING")
                        asking = False

                    return await getattr(conn, cmd)(*args, **kwargs)
                except CONNECTION_ERRORS:
                    try_random_node = True
                    if ttl < self.REQUEST_TTL / 2:
                        await asyncio.sleep(0.1)
                    self._refresh_nodes_asap = True
                except ReplyError as err:
                    parsed_error = parse_cluster_response_error(err)
                    if parsed_error is None:
                        raise
                    if parsed_error.reply == "MOVED":
                        if parsed_error.args is None:
                            raise
                        logger.debug('Got MOVED command: %s', str(err))
                        address = parsed_error.args
                        # Cache this new node if it already exists.
                        node = self._cluster_manager.get_node_by_address(
                            address
                        )
                        if node:
                            slot = self.get_slot(command, *args, **kwargs)
                            self._cluster_manager.slots[slot][0] = node
                        await self.increment_moved_count()
                    elif parsed_error.reply == "TRYAGAIN":
                        if ttl < self.REQUEST_TTL / 2:
                            await asyncio.sleep(0.05)
                    elif parsed_error.reply == "ASK":
                        address = parsed_error.args
                        asking = True
                    elif parsed_error.reply == "CLUSTERDOWN":
                        self._refresh_nodes_asap = True
                        raise
                    else:
                        raise
        finally:
            for conn in connections.values():
                conn.close()
                await conn.wait_closed()

        raise RedisClusterError("TTL exhausted.")

    async def _execute_nodes(self, command, *args, slaves=False, **kwargs):
        """
        Execute redis command for all nodes and returns
        Future waiting for the answer.

        :param command str
        :param slaves bool - Execute on all nodes masters + slaves
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """
        nodes = self._get_nodes_entities(slaves=slaves)
        return await asyncio.gather(*[
            self._execute_node(node, command, *args, **kwargs)
            for node in nodes
        ])

    async def execute(
            self, command, *args, address=None, many=False, slaves=False,
            **kwargs
    ):
        """Execute redis command and returns Future waiting for the answer.

        :param command str
        :param address tuple - Execute on node with specified address
            if many specified will be ignored
        :param many bool - invoke on all master nodes
        :param slaves bool - if many specified, execute even on slave nodes
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """

        if many:
            return await self._execute_nodes(
                command, *args, slaves=slaves, **kwargs
            )

        # If requested, we should refresh before doing anything.
        if self._refresh_nodes_asap:
            async with self._initalize_lock:
                if self._refresh_nodes_asap:
                    await self.initialize()
        if not address:
            address = self.get_node(command, *args, **kwargs).address

        return await self._execute_node(address, command, *args, **kwargs)

    def __getattr__(self, cmd):
        return partial(self.execute, cmd)


class RedisPoolCluster(RedisCluster, ClusterTransactionsMixin):
    """
    Redis pool cluster.
    Do not use it for cluster management.
    Will not operate with slaves and target node
    """

    def __init__(self, nodes, db=0, password=None, encoding=None,
                 *, minsize, maxsize, commands_factory,
                 timeout=None):
        super().__init__(nodes, db=db, password=password, encoding=encoding,
                         commands_factory=commands_factory)
        self._minsize = minsize
        self._maxsize = maxsize
        self._cluster_pool = {}
        self._connection_timeout = timeout

    async def get_cluster_pool(self):
        cluster_pool = {}
        nodes = list(self._cluster_manager.masters)
        tasks = [
            create_redis_pool(
                node.address,
                db=self._db,
                password=self._password,
                encoding=self._encoding,
                minsize=self._minsize,
                maxsize=self._maxsize,
                commands_factory=self._factory,
                timeout=self._connection_timeout,
            )
            for node in nodes
        ]
        results = await asyncio.gather(*tasks)

        for node, connection in zip(nodes, results):
            cluster_pool[node.id] = connection
        return cluster_pool

    async def get_pool_for_node(self, node):
        if node.id in self._cluster_pool:
            return self._cluster_pool[node.id]

        connection = await create_redis_pool(
            node.address,
            db=self._db,
            password=self._password,
            encoding=self._encoding,
            minsize=self._minsize,
            maxsize=self._maxsize,
            commands_factory=self._factory,
            timeout=self._connection_timeout,
        )
        self._cluster_pool[node.id] = connection
        return connection

    async def reload_cluster_pool(self):
        logger.debug('Reloading cluster...')
        await self.clear()
        self._moved_count = 0
        await self.fetch_cluster_info()
        logger.debug('Connecting to cluster...')
        self._cluster_pool = await self.get_cluster_pool()
        logger.debug('Reloaded cluster')

    async def initialize(self):
        await super().initialize()
        self._cluster_pool = await self.get_cluster_pool()

    async def clear(self):
        """Clear pool connections. Close and remove all free connections."""
        for pool in self._cluster_pool.values():
            pool.close()
            await pool.wait_closed()

    async def get_conn_context_for_address(self, address):
        node = self._cluster_manager.get_node_by_address(
            address
        )
        if node:
            pool = await self.get_pool_for_node(node)
            return ClusterConnectionContext(pool)
        await self.initialize_asap()
        node = self._cluster_manager.get_node_by_address(
            address
        )
        if node is None:
            raise RedisClusterError(
                "Could not find node with address {}".format(address)
            )
        pool = await self.get_pool_for_node(node)
        return ClusterConnectionContext(pool)

    async def get_conn_context_for_node(self, node):
        pool = await self.get_pool_for_node(node)
        return ClusterConnectionContext(pool)

    async def get_conn_context_for_slot(self, slot):
        node = self._cluster_manager.get_node_by_slot(slot)
        if not node:
            raise RedisClusterError(
                "No master available for slot {}!".format(slot)
            )
        pool = await self.get_pool_for_node(node)
        return ClusterConnectionContext(pool)

    async def _execute_node(self,
                            address,
                            command,
                            *args,
                            asking=False,
                            **kwargs):
        """Execute redis command and returns Future waiting for the answer.

        :param command str
        :param pool obj
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """
        cmd = decode(command, 'utf-8').lower()
        try_random_node = False
        avoid_address = None
        ttl = int(self.REQUEST_TTL)

        while ttl > 0:
            ttl -= 1

            try:
                if try_random_node:
                    node = self._cluster_manager.get_random_master_node()
                    count = 3
                    while node.address == avoid_address and count > 0:
                        node = self._cluster_manager.get_random_master_node(
                            ignore_address=avoid_address,
                        )
                        count -= 1
                    try_random_node = False
                    avoid_address = None
                else:
                    node = self._cluster_manager.get_node_by_address(address)
                    if node is None:
                        await self.initialize_asap()
                        node = self._cluster_manager.get_node_by_address(
                            address
                        )
                pool = await self.get_pool_for_node(node)

                with await pool as conn:
                    if asking:
                        await conn.execute(b"ASKING")
                        asking = False

                    return await getattr(conn, cmd)(*args, **kwargs)
            except CONNECTION_ERRORS:
                try_random_node = True
                avoid_address = address
                if ttl < self.REQUEST_TTL / 2:
                    await asyncio.sleep(0.1)
                self._refresh_nodes_asap = True
            except ReplyError as err:
                parsed_error = parse_cluster_response_error(err)
                if parsed_error is None:
                    raise
                if parsed_error.reply == "MOVED":
                    if parsed_error.args is None:
                        raise
                    logger.debug('Got MOVED command: %s', str(err))
                    address = parsed_error.args
                    # Cache this new node if it already exists.
                    node = self._cluster_manager.get_node_by_address(
                        address
                    )
                    if node:
                        slot = self.get_slot(command, *args, **kwargs)
                        self._cluster_manager.slots[slot][0] = node
                    await self.increment_moved_count()

                elif parsed_error.reply == "TRYAGAIN":
                    if ttl < self.REQUEST_TTL / 2:
                        await asyncio.sleep(0.05)
                elif parsed_error.reply == "ASK":
                    address = parsed_error.args
                    asking = True
                elif parsed_error.reply == "CLUSTERDOWN":
                    self._refresh_nodes_asap = True
                    raise
                else:
                    raise

        raise RedisClusterError("TTL exhausted.")

    async def execute(self, command, *args, address=None, many=False, **kwargs):
        """Execute redis command and returns Future waiting for the answer.

        :param command str
        :param many bool - invoke on all master nodes
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """

        if many:
            return await self._execute_nodes(command, *args, **kwargs)

        # If requested, we should refresh before doing anything.
        if self._refresh_nodes_asap:
            async with self._initalize_lock:
                if self._refresh_nodes_asap:
                    await self.initialize()

        if address is None:
            node = self.get_node(command, *args, **kwargs)
            address = node.address
        return await self._execute_node(
            address,
            command,
            *args,
            **kwargs,
        )


class ClusterConnectionContext(object):
    """
    Meant to be an easy way to acquire an individual connection to a specific
    node in a cluster. Will take care of closing the connection or releasing it
    back into the pool (depending on what type of cluster it is).
    """

    def __init__(self, redis):
        self._redis = redis
        self._redis_context = None

    async def __aenter__(self):
        self._redis_context = await self._redis
        return self._redis_context

    async def __aexit__(self, exc_type, exc_value, traceback):
        if not self._redis_context:
            return
        if isinstance(self._redis.connection, AbcPool):
            # This is a hacky way to force release the connection.
            with self._redis_context as conn:
                pass
        else:
            self._redis.close()
            await self._redis.wait_closed()
