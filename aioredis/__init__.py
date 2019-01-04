from .connection import RedisConnection, create_connection
from .commands import (
    Redis, create_redis,
    create_redis_pool,
    GeoPoint, GeoMember,
    )
from .pool import ConnectionsPool, create_pool
from .pubsub import Channel
from .sentinel import RedisSentinel, create_sentinel
from .cluster import create_cluster, create_pool_cluster
from .errors import (
    ConnectionClosedError,
    ConnectionForcedCloseError,
    MasterNotFoundError,
    MultiExecError,
    PipelineError,
    ProtocolError,
    ReadOnlyError,
    RedisError,
    ReplyError,
    MaxClientsError,
    AuthError,
    ChannelClosedError,
    WatchVariableError,
    PoolClosedError,
    SlaveNotFoundError,
    MasterReplyError,
    SlaveReplyError,
    )


__version__ = '1.2.0-brilliant.1'

__all__ = [
    # Factories
    'create_connection',
    'create_pool',
    'create_redis',
    'create_redis_pool',
    'create_sentinel',
    'create_cluster',
    'create_pool_cluster',
    # Classes
    'RedisConnection',
    'ConnectionsPool',
    'Redis',
    'GeoPoint',
    'GeoMember',
    'Channel',
    'RedisSentinel',
    # Errors
    'RedisError',
    'ReplyError',
    'MaxClientsError',
    'AuthError',
    'ProtocolError',
    'PipelineError',
    'MultiExecError',
    'WatchVariableError',
    'ConnectionClosedError',
    'ConnectionForcedCloseError',
    'PoolClosedError',
    'ChannelClosedError',
    'MasterNotFoundError',
    'SlaveNotFoundError',
    'ReadOnlyError',
    'MasterReplyError',
    'SlaveReplyError',
]
