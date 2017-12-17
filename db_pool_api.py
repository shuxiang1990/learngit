#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, with_statement

import time
import importlib

from DBUtils.PooledDB import PooledDB

class NoDBConnectorException(Exception):
    pass

class TypeException(Exception):
    pass

class APILevelException(Exception):
    pass

class NotCompatibleException(Exception):
    pass

class NotSupportCursorType(Exception):
    pass

def make_connection(creator="MySQLdb", **kwargs):
    """Make different connections depends on different creator
    
    Typical usage::

        db = make_connection(host=host, db=database, user=user,
                             passwd=password, max_idle_time=max_idle_time,
                             connect_timeout=connect_timeout, creator=creator,
                             mincached=2, ...)
        
        db.query("select * from a")
        db.insertmany(Config.insertmany, [(5,5,'xxxx'),(6,6,'updatemany')])
        db.execute("set autocommit = 0")
        db.begin()
        print db.execute("insert into a values (2,3,'hello')")
        db.commit()
        db.close()

        # set custom cursor
        db.set_cursor("SSDictCursor")
        db.query("select * from a")

        # or you can set cursor on every query
        db.query("select * from a", cs_type="SSDictCursor")

        # the results format between SSDictCursor and DictCursor are different, please check
        db.query("select * from a", cs_type="SSDictCursor")
        db.query("select * from a", cs_type="DictCursor")

        ConnectionPool's args as follows:
        
        creator: either an arbitrary function returning new DB-API 2
            connection objects or a DB-API 2 compliant database module
        mincached: initial number of idle connections in the pool
            (0 means no connections are made at startup)
        maxcached: maximum number of idle connections in the pool
            (0 or None means unlimited pool size)
        maxshared: maximum number of shared connections
            (0 or None means all connections are dedicated)
            When this maximum number is reached, connections are
            shared if they have been requested as shareable.
        maxconnections: maximum number of connections generally allowed
            (0 or None means an arbitrary number of connections)
        blocking: determines behavior when exceeding the maximum
            (if this is set to true, block and wait until the number of
            connections decreases, otherwise an error will be reported)
        maxusage: maximum number of reuses of a single connection
            (0 or None means unlimited reuse)
            When this maximum usage number of the connection is reached,
            the connection is automatically reset (closed and reopened).
        setsession: optional list of SQL commands that may serve to prepare
            the session, e.g. ["set datestyle to ...", "set time zone ..."]
        reset: how connections should be reset when returned to the pool
            (False or None to rollback transcations started with begin(),
            True to always issue a rollback for safety's sake)
        failures: an optional exception class or a tuple of exception classes
            for which the connection failover mechanism shall be applied,
            if the default (OperationalError, InternalError) is not adequate
        ping: determines when the connection should be checked with ping()
            (0 = None = never, 1 = default = whenever fetched from the pool,
            2 = when a cursor is created, 4 = when a query is executed,
            7 = always, and all other bit combinations of these values)

    Args:
        creator: support "MySQLdb", "pymysql", "cx_Oracle" or other 
                 DB-API 2.0 compatible connectors, must be string
                 The default is MySQLdb, support MySQL or Oracle
        kwargs: args used by Connection. creator's args and connection pool's args
    
    Return: 
        a warpped Connection
    
    Exception：
        NoDBConnectorException,NotCompatibleException,APILevelException,TypeException
    """
    if not isinstance(creator, basestring):
        raise TypeException("creator must be a string")

    driver = creator.strip().lower()

    if driver == "cx_oracle":
        try:
            import cx_Oracle as connector
        except ImportError:
            raise NoDBConnectorException("Please install correct oracle connector")
    elif driver == "mysqldb":
        try:
            import MySQLdb as connector
        except ImportError:
            raise
    elif driver == "pymysql":
        try:
            import pymysql as connector
        except ImportError:
            raise
    else:
        try:
            connector = importlib.import_module(driver)
        except ImportError:
            raise NoDBConnectorException("Please install correct database connector %s" % creator)

    if hasattr(connector, "apilevel"):
        if connector.apilevel != "2.0":
            raise APILevelException("%s's apilevel is 1.0, only support db api 2.0" % connector.__name__ )

    else:
        raise NotCompatibleException("%s is not db api 2.0 compatible" % connector.__name__)

    kwargs["creator"] = connector

    return _Connection(**kwargs)


class _Connection(object):
    """A lightweight wrapper around MySQLdb DB-API connections.
      
    """
    def __init__(self, **kwargs):
        
        host = kwargs.pop("host", "127.0.0.1")
        charset = kwargs.pop("charset", "utf8")
        db = kwargs.pop("db", "test")
        connect_timeout = kwargs.pop("connect_timeout", 1)
        user = kwargs.pop("user", "root")
        passwd = kwargs.pop("passwd", None)
        use_unicode = kwargs.pop("use_unicode", True)
        self._max_idle_time = float(kwargs.pop("max_idle_time", 7 * 3600))
        self._creator = kwargs.get("creator")
        self.cursor = "Cursor"

        args = dict(use_unicode=use_unicode, charset=charset, db=db,
                    connect_timeout=connect_timeout, user=user, passwd=passwd)

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        self._db_args.update(kwargs)

        self._pool = _ConnectionPool(**self._db_args)

        self.reconnect()

    def reconnect(self):
        """Closes the existing database connection and re-opens it.
        
        """
        self.close()
        self._db = self._pool.get_connection()

    def query(self, query, cs_type=None, *parameters, **kwparameters):
        """Returns a custom result for the given query and parameters.
        
        """
        cursor = self._cursor(cs_type)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            if cs_type in ["SSCursor", "SSDictCursor"]:
                while 1:
                    try:
                        row = cursor.fetchone()
                    except Exception, e:
                        cursor.close()
                        raise e
                    if row:
                        yield row
                    else:
                        break
            else:
                yield [Row(row) if isinstance(row, dict) else row for row in cursor]
        except Exception, e:
            cursor.close()

    def execute(self, query, cs_type=None, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query.
        
            rowcount is a more reasonable default return value than lastrowid,
            but for historical compatibility execute() must return lastrowid.
        """
        return self.execute_lastrowid(query, cs_type, *parameters, **kwparameters)

    def execute_lastrowid(self, query, cs_type=None, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query.
        
        """
        cursor = self._cursor(cs_type)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, cs_type=None, *parameters, **kwparameters):
        """Executes the given query, returning the affected rowcount from the query.
        
        """
        cursor = self._cursor(cs_type)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, *parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, *parameters)

    def executemany_lastrowid(self, query, *parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        cursor = self._cursor(None)
        try:
            cursor.executemany(query, *parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, *parameters):
        """Executes the given query against all the given param sequences.

        We return the rowcount from the query.
        """
        cursor = self._cursor(None)
        try:
            cursor.executemany(query, *parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    update = delete = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    def close(self):
        """Close the connection and reclaim to connection pool

        """
        if getattr(self, "_db", None):
            self._db.close()
            self._db = None

    def begin(self):

        if self._db:
            self._db.begin()

    def commit(self):

        if self._db:
            self._db.commit()

    def rollback(self):

        if self._db:
            self._db.rollback()

    def set_cursor(self, cs_type):

        self.cursor = cs_type

    def _ensure_connected(self):
        '''Mysql by default closes client connections that are idle for
        8 hours, but the client library does not report this fact until
        you try to perform a query and it fails.  Protect against this
        case by preemptively closing and reopening the connection
        if it has been idle for too long (7 hours by default).
        '''
        if (self._db is None or (time.time() - self._last_use_time > self._max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self, cs_type):
        """Returns typical cursor
        
        """
        self._ensure_connected()
        if not cs_type:
            cursor = self.cursor
        else:
            cursor = cs_type
        try:
            cs_type = getattr(self._creator.cursors, cursor)
            return self._db.cursor(cursorclass=cs_type)
        except AttributeError, e:
            raise NotSupportCursorType("%s not supported" % cs_type)

    def _execute(self, cursor, query, parameters, kwparameters):

        try:
            return cursor.execute(query, kwparameters or parameters)
        except Exception, e:
            self.close()

    def __del__(self):

        self.close()

class Row(dict):
    """A dict that allows for object-like property access syntax.
    
    """
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

class _Singleton(object):

    _instance = None

    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = super(_Singleton, cls).__new__(cls, *args, **kw)
        return cls._instance

class _ConnectionPool(_Singleton):

    def __init__(self, **kwargs):
        """Prepare DButils' args then get rid of them from kwargs 
            in case of polluting creator's(MySQLdb or pymysql) args
            
        """
        creator = kwargs.pop("creator", None)
        if not creator:
            import MySQLdb
            creator = MySQLdb
        mincached = kwargs.pop("mincached", 2)
        maxcached = kwargs.pop("maxcached", 10)
        maxshared = kwargs.pop("maxshared", 10)
        maxconnections = kwargs.pop("maxconnections", 20)
        blocking = kwargs.pop("blocking", 0)
        reset = kwargs.pop("reset", True)
        maxusage = kwargs.pop("maxusage", 0)
        setsession = kwargs.pop("setsession", ["set autocommit = 0"])
        ping = kwargs.pop("ping", 1)

        self._pool = PooledDB(creator=creator, mincached=mincached, maxcached=maxcached,
                               maxshared=maxshared, maxconnections=maxconnections,
                               blocking=blocking, maxusage=maxusage,reset=reset,
                               setsession=setsession, ping=ping, **kwargs)

    def get_connection(self, shareable=False):

        return self._pool.connection(shareable)
