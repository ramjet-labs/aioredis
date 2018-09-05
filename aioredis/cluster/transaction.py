import asyncio
import functools

from ..errors import PipelineError, RedisError, ReplyError
from .util import parse_cluster_response_error


class InvalidPipelineOperation(RedisError):
    """
    Raised when a pipeline or transaction attempts to execute a series of
    commands that do not belong to the same node (pipeline) or same hash slot
    (transaction).
    """
    pass


class ClusterTransactionsMixin(object):
    """
    Defines pipeline/transaction specific commands for cluster.
    """

    def pipeline(self):
        """
        Returns :class:`ClusterPipeline` object to execute bulk commands.

        Note that all commands in a pipeline must be executed on the same node.

        Usage:

        >>> pipeline = cluster.pipeline()
        >>> fut1 = pipe.incr('{key}foo')  # No `await` as it will block forever!
        >>> fut2 = pipe.incr('{key}foo2')
        >>> result = await pipeline.execute()
        >>> result_check = await asyncio.gather(fut1, fut2)
        >>> assert result == result_check
        """
        return ClusterPipeline(self, commands_factory=self._factory,
                               loop=self._loop)


class _RedisBuffer(object):

    def __init__(self, pipeline, cluster, *, loop=None, force_same_slot=False):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._pipeline = pipeline
        self._cluster = cluster
        self._loop = loop
        self._force_same_slot = force_same_slot
        self.node = None
        self.slot = None

    def execute(self, cmd, *args, **kwargs):
        if self._force_same_slot:
           expected_slot = self._cluster.get_slot(cmd, *args, **kwargs)
           if self.slot is None or expected_slot == self.slot:
               self.slot = expected_slot
           else:
                raise InvalidPipelineOperation(
                    "All keys in pipeline must belong to the same slot!"
                )
        else:
            expected_node = self._cluster.get_node(cmd, *args, **kwargs)
            if self.node is None or expected_node == self.node:
                self.node = expected_node
            else:
                raise InvalidPipelineOperation(
                    "All keys in pipeline must point to same node!"
                )

        fut = self._loop.create_future()
        self._pipeline.append((fut, cmd, args, kwargs))
        return fut


class ClusterPipeline(object):
    """Commands pipeline for a redis cluster.

    Currently, the pipeline only allows for operations on keys whos slots all
    map to the same node. If an operation is attempted on set of key(s) that
    would map to a slot on a different node than the already pipelined
    operation, then a InvalidPipelineOperation will be raised, and all existing
    pipelined operations will be cancelled.
    """

    error_class = PipelineError

    def __init__(self, cluster, commands_factory=lambda cluster: cluster,
                 *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._cluster = cluster
        self._loop = loop
        self._pipeline = []
        self._results = []
        self._buffer = _RedisBuffer(self._pipeline, cluster=cluster, loop=loop)
        self._redis = commands_factory(self._buffer)
        self._done = False

    def __getattr__(self, name):
        assert not self._done, "Pipeline already executed. Create new one."
        attr = getattr(self._redis, name)
        if callable(attr):

            @functools.wraps(attr)
            def wrapper(*args, **kw):
                try:
                    prev_pipeline_len = len(self._pipeline)
                    task = asyncio.ensure_future(attr(*args, **kw),
                                                 loop=self._loop)
                except InvalidPipelineOperation:
                    # If one step in the pipeline is invalid, we should cancel
                    # all the existing futures and exit.
                    self._cancel_pending_futures()
                    self._done = True
                    raise
                except Exception as exc:
                    task = self._loop.create_future()
                    task.set_exception(exc)
                # If we added a new command to the pipeline, let's store the
                # actual command + args/kwargs in the results list as well (so
                # that we can retry if necessary).
                if len(self._pipeline) > prev_pipeline_len:
                    _, cmd, cmd_args, cmd_kwargs = self._pipeline[-1]
                else:
                    cmd, cmd_args, cmd_kwargs = None, None, None
                self._results.append((task, cmd, cmd_args, cmd_kwargs))
                return task
            return wrapper
        return attr

    def _cancel_pending_futures(self):
        for fut, _, _, _ in self._pipeline:
            fut.cancel()
        for fut, _, _, _ in self._results:
            fut.cancel()

    async def execute(self, *, return_exceptions=False):
        """Execute all buffered commands.

        Any exception that is raised by any command is caught and
        raised later when processing results.

        Exceptions can also be returned in result if
        `return_exceptions` flag is set to True.
        """
        assert not self._done, "Pipeline already executed. Create new one."
        self._done = True

        if self._pipeline:
            conn_context = await self._cluster.get_conn_context_for_node(
                self._buffer.node
            )
            async with conn_context as conn:
                return await self._do_execute(
                    conn,
                    return_exceptions=return_exceptions
                )
        else:
            return await self._gather_result(return_exceptions)

    async def _do_execute(self, conn, *, return_exceptions=False):
        await asyncio.gather(*self._send_pipeline(conn),
                             loop=self._loop,
                             return_exceptions=True)
        return await self._gather_result(return_exceptions)

    async def _gather_result(self, return_exceptions):
        errors = []
        results = []
        for fut, cmd, args, kwargs in self._results:
            try:
                res = await fut
                results.append(res)
            except ReplyError as e:
                # If we get a MOVED or ASK back, we should send the command as
                # we normally would using the cluster.
                parsed_err = parse_cluster_response_error(e)
                if parsed_err:
                    if parsed_err.reply in ["MOVED", "ASK"]:
                        kwargs.update({
                            "address": parsed_err.args,
                            "asking": parsed_err.reply == "ASK",
                        })
                        try:
                            result = await self._cluster.execute(
                                cmd,
                                *args,
                                **kwargs,
                            )
                            results.append(result)
                        except Exception as exc:
                            errors.append(exc)
                            results.append(exc)
                    else:
                        errors.append(e)
                        results.append(e)
                else:
                    errors.append(e)
                    results.append(e)
            except Exception as exc:
                errors.append(exc)
                results.append(exc)
        if errors and not return_exceptions:
            raise self.error_class(errors)
        return results

    def _send_pipeline(self, conn):
        for fut, cmd, args, kw in self._pipeline:
            try:
                result_fut = conn.execute(cmd, *args, **kw)
                result_fut.add_done_callback(
                    functools.partial(self._check_result, waiter=fut))
            except Exception as exc:
                fut.set_exception(exc)
            else:
                yield result_fut

    def _check_result(self, fut, waiter):
        if fut.cancelled():
            waiter.cancel()
        elif fut.exception():
            waiter.set_exception(fut.exception())
        else:
            waiter.set_result(fut.result())
