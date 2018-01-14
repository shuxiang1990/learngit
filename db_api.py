#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, with_statement

import time
import logging
import copy
import os
import sys

try:
    import pymysql as MySQLdb
except ImportError:
    try:
        import MySQLdb
    except ImportError:
        raise
try:
    import MySQLdb.constants
    import MySQLdb.converters
    import MySQLdb.cursors
except ImportError:
    # If MySQLdb isn't available this module won't actually be useable,
    # but we want it to at least be importable on readthedocs.org,
    # which has limitations on third-party modules.
    if 'READTHEDOCS' in os.environ:
        MySQLdb = None
    else:
        raise

class ConnectionHangError(MySQLdb.OperationalError):
    def __init__(self, *args, **kwargs):
        pass

class NotSupportCursorType(Exception):
    pass

def session(**kwargs):
    """
    Typical usage::
        db = session(host=host, db=database, user=user,
                             passwd=password, max_idle_time=max_idle_time,
                             connect_timeout=connect_timeout)

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
    Args:
        kwargs: args used by Connection
    Return: 
        a warpped db session

    """
    return Connection(**kwargs)

class Connection(object):
    """A lightweight wrapper around MySQLdb DB-API connections.

    """

    def __init__(self, **kwargs):

        host = kwargs.pop("host", "127.0.0.1:3306")
        charset = kwargs.pop("charset", "utf8")
        db = kwargs.pop("db", "test")
        connect_timeout = kwargs.pop("connect_timeout", 3)
        user = kwargs.pop("user", None)
        passwd = kwargs.pop("passwd", None)
        use_unicode = kwargs.pop("use_unicode", True)
        timezone = kwargs.pop("time_zone", "+8:00")
        sqlmode = kwargs.pop("sql_mode", "")
        max_retry = kwargs.pop("max_retry", 3)
        self._max_idle_time = float(kwargs.pop("max_idle_time", 7 * 3600))
        self.cursor = "Cursor"
        self.max_retry = max_retry

        args = dict(conv=CONVERSIONS, use_unicode=use_unicode, charset=charset,
                    db=db, init_command=('SET time_zone = "%s"' % timezone),
                    connect_timeout=connect_timeout, user=user, passwd=passwd, sql_mode=sqlmode)

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

        if MySQLdb.version_info >= (1, 2, 5):
            args["read_timeout"] = args["read_timeout"] if args.get("read_timeout") else 15
            args["write_timeout"] = args["write_timeout"] if args.get("write_timeout") else 10
        else:
            if 'read_timeout' in args:
                del args['read_timeout']
            if 'write_timeout' in args:
                del args['write_timeout']

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        self._db_args.update(kwargs)

        self.reconnect()

    def reconnect(self):
        """Closes the existing database connection and re-opens it.

        """
        self.close()
        self._db = MySQLdb.connect(**self._db_args)
        self._db.autocommit(True)

    def iter(self, query, cs_type=None, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = self._cursor(cs_type)
        try:
            for idx in range(self.max_retry):
                self._execute(cursor, query, parameters, kwparameters)
                if cursor.description is not None:
                    break
                else:
                    time.sleep(0.1)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, cs_type=None, *parameters, **kwparameters):
        """Returns a custom result for the given query and parameters.

        """
        retry_time = self.max_retry
        while True:
            cursor = self._cursor(cs_type)
            try:
                self._execute(cursor, query, parameters, kwparameters)
                column_names = [d[0] for d in cursor.description]
                return [Row(zip(column_names, row)) for row in cursor]
            except TypeError:
                self.reconnect()
                retry_time -= 1
                if retry_time < 0:
                    raise
            finally:
                cursor.close()

    def get(self, query, cs_type=None, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, cs_type, *parameters, **kwparameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

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
        cursor = self._cursor(None)
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
            cs_type = getattr(MySQLdb.cursors, cursor)
            return self._db.cursor(cursorclass=cs_type)
        except AttributeError, e:
            raise NotSupportCursorType("%s not supported" % cs_type)

    def _execute(self, cursor, query, parameters, kwparameters):
        retry_time = self.max_retry
        while True:
            tid = self._db.thread_id()
            try:
                return cursor.execute(query, kwparameters or parameters)
            except MySQLdb.OperationalError, e:
                logging.error("Error connecting to MySQL on %s", self.host)
                self.close()
                time.sleep(0.5)
                self.reconnect()
                cursor = self._cursor()
                if e.args:
                    # (2003, "Can't connect to MySQL server on 'xxx.xxx.xxx.xxx' (110))
                    # only retry once
                    if e.args[0] == 2003 and retry_time < self.max_retry:
                        raise
                    # (2013, Lost connection to MySQL server during query) and kill old session
                    elif e.args[0] == 2013 and tid in self.thread_ids():
                        self._db.kill(tid)
                retry_time -= 1
                if retry_time < 1:
                    if e.args and e.args[0] == 2013:
                        raise ConnectionHangError('%s is hang!!!' % self.host)
                    raise

    def __del__(self):
        self.close()

    def thread_ids(self):
        cursor = self._cursor()
        try:
            cursor.execute('SELECT ID FROM information_schema.PROCESSLIST WHERE USER = %s', (self.user,))
            column_names = [d[0] for d in cursor.description]
            return [tid["ID"] for tid in [Row(zip(column_names, row)) for row in cursor]]
        finally:
            cursor.close()


class Row(dict):
    """A dict that allows for object-like property access syntax.

    """

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


if MySQLdb is not None:
    # Fix the access conversions to properly recognize unicode/binary
    FIELD_TYPE = MySQLdb.constants.FIELD_TYPE
    FLAG = MySQLdb.constants.FLAG
    CONVERSIONS = copy.copy(MySQLdb.converters.conversions)
    field_types = [FIELD_TYPE.BLOB, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING]
    if 'VARCHAR' in vars(FIELD_TYPE):
        field_types.append(FIELD_TYPE.VARCHAR)
    if 'pymysql' not in sys.modules:
        for field_type in field_types:
            CONVERSIONS[field_type] = [(FLAG.BINARY, str)] + CONVERSIONS[field_type]
    # Alias some common MySQL exceptions
    IntegrityError = MySQLdb.IntegrityError
    OperationalError = MySQLdb.OperationalError
