# -*- coding: utf-8 -*-
from __future__ import absolute_import
from rpc_thrift.utils import get_base_protocol_4_pool
import contextlib
import logging
import Queue


logger = logging.getLogger(__name__)

#
# TODO: maybe support multiple Thrift servers. What would a reasonable
# distribution look like? Round-robin? Randomize the list upon
# instantiation and then cycle through it? How to handle (temporary?)
# connection errors?
#


class NoConnectionsAvailable(RuntimeError):
    """
    Exception raised when no connections are available.

    This happens if a timeout was specified when obtaining a connection,
    and no connection became available within the specified timeout.

    .. versionadded:: 0.5
    """
    pass


class ConnectionPool(object):
    """
    Thread-safe connection pool.

    .. versionadded:: 0.5

    The `size` argument specifies how many connections this pool
    manages. Additional keyword arguments are passed unmodified to the
    :py:class:`happybase.Connection` constructor, with the exception of
    the `autoconnect` argument, since maintaining connections is the
    task of the pool.

    :param int size: the maximum number of concurrently open connections
    :param kwargs: keyword arguments passed to
                   :py:class:`happybase.Connection`
    """
    def __init__(self, size, endpoint):
        if not isinstance(size, int):
            raise TypeError("Pool 'size' arg must be an integer")

        if not size > 0:
            raise ValueError("Pool 'size' arg must be greater than zero")

        logger.debug("Initializing connection pool with %d connections", size)

        self._queue = Queue.LifoQueue(maxsize=size)

        for i in xrange(size):
            connection = get_base_protocol_4_pool(endpoint)
            self._queue.put(connection)


    def _acquire_connection(self, timeout=None):
        """Acquire a connection from the pool."""
        try:
            return self._queue.get(True, timeout)
        except Queue.Empty:
            raise NoConnectionsAvailable("No base protocol available from pool within specified timeout")

    def _return_connection(self, connection):
        """Return a connection to the pool."""
        self._queue.put(connection)

    @contextlib.contextmanager
    def connection(self, timeout=None):
        """
        Obtain a connection from the pool.

        This method *must* be used as a context manager, i.e. with
        Python's ``with`` block. Example::

            with pool.connection() as connection:
                pass  # do something with the connection

        If `timeout` is specified, this is the number of seconds to wait
        for a connection to become available before
        :py:exc:`NoConnectionsAvailable` is raised. If omitted, this
        method waits forever for a connection to become available.

        :param int timeout: number of seconds to wait (optional)
        :return: active connection from the pool
        :rtype: :py:class:`happybase.Connection`
        """
        connection = self._acquire_connection(timeout)
        yield connection
        self._return_connection(connection)
