import asyncio
import functools

from ..errors import (ConnectionClosedError, MultiExecError, PipelineError,
                      RedisError, ReplyError)
from ..util import _set_exception
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

    async def transaction(self, coro, *watched_keys):
        """
        Creates a transaction that will watch the specified keys. Note that
        execute must be called explicitly. In addition, all watched keys and all
        keys in the transaction's operations must hash to the same slot.

        Since the transaction will automatically retry certain errors, it is not
        necessary to record the futures of the individual operations and compare
        the result with the result of execute.

        This function also requires a coroutine which given a cluster
        object and transaction object, should fetch keys from the cluster and
        conduct various checks, before pipelining commands into the transaction.
        If this function raises an exception or returns False, then any watched
        keys will be unwatched, and the transaction will not execute.
        As an example:

            async def _coro(cluster, transaction):
                key1 = await cluster.get("key1")
                if key1 != "expected_value":
                    return False
                key2 = await cluster.get("key2")
                if key2 != "expected_value2":
                    raise Exception("Got wrong key2")
                transaction.incr("{key}"foo")
                transaction.incr("{key}"foo2")
                transaction.set("{key}3", key1)

        Usage:
        >>> transaction = await cluster.transaction(
                _coro,
                "{key}foo",
                "{key}foo2",
            )
        """
        multi_exec = ClusterMultiExec(cluster=self,
                                      watched_keys=watched_keys,
                                      coro=coro,
                                      commands_factory=self._factory,
                                      loop=self._loop)
        return await multi_exec.execute()


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
        self._commands = []

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
                self._commands.append((name, args, kw))
                return task
            return wrapper
        return attr

    def _cancel_pending_futures(self):
        for fut, _, _, _ in self._pipeline:
            if not fut.done():
                fut.cancel()
        for fut, _, _, _ in self._results:
            if not fut.done():
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
                        if parsed_err.reply == "MOVED":
                            await self._cluster.increment_moved_count()
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
                    elif parsed_err.reply == "CLUSTERDOWN":
                        await self._cluster.initialize_asap()
                        errors.append(e)
                        results.append(e)
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


class ClusterMultiExec(ClusterPipeline):
    """Transaction pipeline for a redis cluster.

    Transactions only allow for operations on keys that all map to the same
    slot. If an operation is attempted on set of key(s) that
    would map to a slot that is different than the existing operations, then an
    InvalidPipelineOperation will be raised, and all existing pipelined
    operations will be cancelled.
    """

    error_class = MultiExecError
    MAX_EXECUTION_ATTEMPTS = 10

    def __init__(self,
                 cluster,
                 watched_keys,
                 coro,
                 commands_factory=lambda cluster: cluster,
                 *,
                 loop=None):
        """
        Creates a transaction pipeline:

        cluster: The cluster against which the transaction should be run.
        watched_keys: A list of keys to be watched (can be empty).
        coro: A coroutine that should be run between the time the
            keys are watched and the transaction is executed. This
            function should accept a cluster object and transaction object, and
            should execute any fetches using the cluster object, and pipeline
            any operations into the transaction. If this function raises an
            exception or False, then any watched keys will be unwatched, and the
            transaction will not execute. If all checks succeed, the function
            should return True.
        """
        super().__init__(cluster, commands_factory=commands_factory, loop=loop)
        self._buffer = _RedisBuffer(
            self._pipeline,
            cluster=cluster,
            loop=loop,
            force_same_slot=True,
        )
        self._redis = commands_factory(self._buffer)
        self._watched_keys = watched_keys
        if watched_keys:
            # Force the buffer slot so all future pipelined requests are
            # verified compared to the WATCH.
            self._buffer.slot = self._cluster.get_slot("WATCH", *watched_keys)
        self._coro = coro

    def watch(self, *keys):
        raise InvalidPipelineOperation(
            "Cannot watch after starting transaction."
        )

    async def execute(self, *, return_exceptions=False):
        """Execute all buffered commands.

        Any exception that is raised by any command is caught and
        raised later when processing results.

        Exceptions can also be returned in result if
        `return_exceptions` flag is set to True.
        """
        assert not self._done, "Pipeline already executed. Create new one."

        try:
            return await self._execute_pipeline(return_exceptions)
        finally:
            self._cancel_pending_futures()
            self._done = True

    async def _get_conn_context(self, slot, address):
        if address:
            conn_context = await self._cluster.get_conn_context_for_address(
                address
            )
        else:
            conn_context = await self._cluster.get_conn_context_for_slot(
                slot
            )
        return conn_context

    async def _execute_pipeline(self,
                                return_exceptions,
                                retries=3):
        """
        Actually execute the pipeline. This will handle processing any possible
        MOVED requests that occur during execution of the pipeline.
        """
        address = None
        while retries > 0:
            retries -= 1
            # Reset the pipeline and result tasks. This is so when we execute
            # the transaction's coro it can safely re-populate the pipeline and
            # results tasks.
            self._reset_pipeline_and_results()
            # If we are not watching any keys, we should set up the pipeline
            # immediately.
            if not self._watched_keys:
                if await self._coro(self._cluster, self) is False:
                    return []
                # If no pipeline was created in the coro, then we should short
                # circuit and gather whatever we have.
                if not self._pipeline:
                    return await self._gather_result(return_exceptions)
            conn_context = await self._get_conn_context(
                self._buffer.slot,
                address,
            )
            async with conn_context as conn:
                try:
                    # An exception will be raised if the WATCH or MULTI fails.
                    results = await self._do_execute(
                        conn,
                        return_exceptions=True,
                    )

                except ReplyError as e:
                    parsed_err = parse_cluster_response_error(e)
                    if not parsed_err:
                        raise
                    if parsed_err.reply == "CLUSTERDOWN":
                        await self._cluster.initialize_asap()
                        raise
                    if parsed_err.reply == "MOVED":
                        address = parsed_err.args
                        await self._cluster.increment_moved_count()
                        await self._cluster.update_node_for_slot(
                            address,
                            self._buffer.slot
                        )
                        continue
                    else:
                        raise
                else:
                    results = await self._process_pipeline_results(
                        return_exceptions,
                        results,
                        self._buffer.slot,
                        address,
                    )
                    # If we are expected to retry, do so, otherwise exit
                    # immediately with the results.
                    if results[0]:
                        address = results[1]
                    else:
                        return results[1]

        raise MultiExecError("Could not execute transaction!")

    async def _process_pipeline_results(self,
                                        return_exceptions,
                                        results,
                                        slot,
                                        address):
        # If any of the requests returned a ReplyError that was MOVED,
        # we can assume that all of them did, since all keys in the transaction
        # must map to the same slot. Reconstruct the pipeline/results and re-try
        # the request with the updated address.
        errors = []
        for result in results:
            if isinstance(result, ReplyError):
                parsed_err = parse_cluster_response_error(result)
                if not parsed_err:
                    errors.append(result)
                elif parsed_err.reply == "MOVED":
                    address = parsed_err.args
                    await self._cluster.increment_moved_count()
                    await self._cluster.update_node_for_slot(address, slot)
                    return True, address
                elif parsed_err.reply == "CLUSTERDOWN":
                    await self._cluster.initialize_asap()
                    errors.append(result)
                else:
                    errors.append(result)
            elif isinstance(result, Exception):
                errors.append(result)

        if errors and not return_exceptions:
            raise self.error_class(errors)
        return False, results

    def _reset_pipeline_and_results(self):
        """Construct a new pipeline and results list based off the old one."""
        self._cancel_pending_futures()
        self._pipeline = []
        self._buffer._pipeline = self._pipeline
        self._results = []

    async def _do_execute(self, conn, *, return_exceptions=False):
        self._waiters = waiters = []
        # Let this error bubble up.
        if self._watched_keys:
            await conn.execute("WATCH", *self._watched_keys)

            try:
                # The coro will have already been executed if we didn't watch
                # any keys.
                if await self._coro(self._cluster, self) is False:
                    await conn.unwatch()
                    return []
            except:
                await conn.unwatch()
                raise

            # If no pipeline was created in the coro, then we should short
            # circuit and gather whatever we have.
            if not self._pipeline:
                return await self._gather_result(return_exceptions)
        multi = conn.execute("MULTI")

        coros = list(self._send_pipeline(conn))
        exec_ = conn.execute("EXEC")
        gather = asyncio.gather(multi, *coros, loop=self._loop,
                                return_exceptions=True)
        last_error = None
        try:
            await asyncio.shield(gather, loop=self._loop)
        except asyncio.CancelledError:
            await gather
        except Exception as err:
            last_error = err
            raise
        finally:
            if conn.closed:
                if last_error is None:
                    last_error = ConnectionClosedError()
                for fut in waiters:
                    _set_exception(fut, last_error)
                for fut, _, _, _ in self._results:
                    if not fut.done():
                        fut.set_exception(last_error)
            else:
                try:
                    results = await exec_
                except RedisError as err:
                    for fut in waiters:
                        fut.set_exception(err)
                else:
                    assert len(results) == len(waiters), (
                        "Results does not match waiters",
                        results,
                        waiters,
                    )
                    self._resolve_waiters(results, return_exceptions)
            return (await self._gather_result(return_exceptions))

    async def _gather_result(self, return_exceptions):
        errors = []
        results = []
        for fut, _, _, _ in self._results:
            try:
                res = await fut
                results.append(res)
            except Exception as exc:
                errors.append(exc)
                results.append(exc)
        if errors and not return_exceptions:
            raise self.error_class(errors)
        return results

    def _resolve_waiters(self, results, return_exceptions):
        errors = []
        for val, fut in zip(results, self._waiters):
            if isinstance(val, RedisError):
                fut.set_exception(val)
                errors.append(val)
            else:
                fut.set_result(val)
        if errors and not return_exceptions:
            raise MultiExecError(errors)

    def _check_result(self, fut, waiter):
        assert waiter not in self._waiters, (fut, waiter, self._waiters)
        assert not waiter.done(), waiter
        # await gather was cancelled.
        if fut.cancelled():
            waiter.cancel()
        # server replied with error
        elif fut.exception():
            waiter.set_exception(fut.exception())
        elif fut.result() in {b"QUEUED", "QUEUED"}:
            self._waiters.append(waiter)
