import time

from aioredis.cluster.testcluster import TestCluster

cluster = TestCluster(list(range(7001, 7007)), '/tmp/rediscluster')
cluster.setup()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    cluster.terminate()
    cluster.clear_directories()
