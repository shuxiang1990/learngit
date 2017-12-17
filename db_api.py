#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, with_statement

import time
try:
    import pymysql as MySQLdb
except ImportError:
    try:
        import MySQLdb
    except ImportError:
        raise

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
        autocommit = kwargs.pop("autocommit", True)
        self._max_idle_time = float(kwargs.pop("max_idle_time", 7 * 3600))
        self.cursor = "Cursor"

        args = dict(use_unicode=use_unicode, charset=charset, db=db, autocommit=autocommit,
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

        self.reconnect()

    def reconnect(self):
        """Closes the existing database connection and re-opens it.
        
        """
        self.close()
        self._db = MySQLdb.connect(**self._db_args)
        #self._db.autocommit(False)

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
