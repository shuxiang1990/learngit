#! /usr/local/sinasrv2/bin/python2.7
# -*- coding: utf-8 -*-
'''
created on 2016.05.01 by shuxiang <shuxiang@staff.sina.com.cn>

'''
__author__ = "shuxiang"

import MySQLdb
import math


class Session(object):

    def __init__(self, *args, **kargs):
        # exception is passed to upper layer
        self._args = args
        self._kargs = kargs
        self._conn = None
        self._cursor = None

    def _reconnect(self):
        '''Close the existing conn and create a new, lazy connect. Exception would be passed to upper layer if had one.

            WARNNING:
            This is not a thread safe method
        '''
        self.close()
        self._conn = Connection(*self._args, **self._kargs).get_connection()
        self._conn.autocommit(False)

    def _get_cursor(self, **kargs):
        '''Return a cursor
        '''
        cursor_class = kargs.get("cursor_class", None)
        if self._conn is None:
            self._reconnect()
        try:
            self._conn.ping()
        except:
            self._reconnect()
        try:
            if cursor_class:
                self._cursor = self._conn.cursor(cursorclass=cursor_class)
            else:
                self._cursor = self._conn.cursor()
        except Exception, e:
            raise
        return self._cursor

    def _execute(self, cursor, sql, *args):
        try:
            cursor.execute(sql, *args)
        except Exception, e:
            self.close()
            raise Exception("query sql<%s> failed: %s" % (sql, str(e)))

    def query(self, sql, *args, **kargs):
        '''Query data from MySQL. Returns a iterator that contains all the fetched rows
            SUGGEST:
            If the result set can be very large, consider adding a LIMIT clause to your query,
            or using CursorUseResultMixIn instead.
            WARNNING:
            When the results is stored on the server, for example 'cursor_class' is CursorUseResultMixIn,
            you must fetch all the data before next query
        '''
        cursor = self._get_cursor(**kargs)
        try:
            self._execute(cursor, sql, *args)
            return (row for row in cursor)
        finally:
            cursor.close()

    def execute_tans(self, sql, *args, **kargs):
        cursor = self._get_cursor(**kargs)
        try:
            self._execute(cursor, sql, *args)
            self.commit()
        finally:
            cursor.close()

    def execute(self, sql, *args, **kargs):
        cursor = self._get_cursor(**kargs)
        try:
            self._execute(cursor, sql, *args)
        finally:
            cursor.close()

    def execute_many(self, sql, *args, **kargs):
        '''

                This method improves performance on multiple-row INSERT and REPLACE.
                Otherwise it is equivalent to looping over args with execute().
        '''
        cursor = self._get_cursor(**kargs)
        try:
            cursor.executemany(sql, *args)
            self.commit()
        except Exception, e:
            raise Exception("executemany failed: %s" % str(e))

    def commit(self):
        if self._conn is not None:
            try:
                self._conn.ping()
            except:
                self._reconnect()
            try:
                self._conn.commit()
            except Exception, e:
                self._conn.rollback()
                raise Exception("commit failed: %s" % str(e))

    def rollback(self):
        if self._conn is not None:
            try:
                self._conn.rollback()
            except Exception, e:
                raise Exception("rollback failed: %s" % str(e))

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()

class Connection(object):

    '''This is a lightweight MySQLdb wrapper

        1: check MySQLdb's parameters and defaults
            MySQLdb.connect(param...) returns a connection, the important parameters are:
            PARAMS              DEFAULT
            host                unix socket where applicable
            user                current effective user
            passwd              no passwd
            db                  no default database
            port                3306
            unix_socket         use default location
            conv                a copy of MySQLdb.converters.conversions
            compress            no compression
            connect_timeout     no timeout ?
            init_command        nothing
            read_default_file   see mysql_options()
            read_default_group  see mysql_options()
            cursorclass         MySQLdb.cursors.Cursor
            use_unicode         True
            charset             ?
            sql_mode
            ssl
        2: use like this
    '''
    def __init__(self, *args, **kargs):

        self.args = args
        self.kargs = kargs
        ## check and set default params
        # if both 'host' and 'unix_socket' are set, use the 'unix_socket'
        # TODO: it should return error or warnning ?
        if kargs.get("host", None) and kargs.get("unix_socket", None):
            self.kargs.pop("host")
        # set a default db
        # TODO: it is better to choose 'mysql'? cause 'test' is gone in MySQL 5.7
        self.kargs["db"] = kargs.get("db", "test")
        # set default connect timeout to 3s
        self.kargs["connect_timeout"] = kargs.get("connect_timeout", 3)

        # pre set mysql option
        self.interactive_timeout = kargs.get("interactive_timeout", None)
        self.wait_timeout = kargs.get("wait_timeout", None)

        try:
            self.kargs.pop("interactive_timeout")
            self.kargs.pop("wait_timeout")
        except:
            pass

    def get_connection(self):
        conn = None

        retry_cnt = 5
        while retry_cnt > 0:
            try:
                conn = MySQLdb.connect(*self.args, **self.kargs)
                if self.wait_timeout or self.interactive_timeout:
                    max_timeout = max(self.interactive_timeout, self.wait_timeout)
                    cursor = conn.cursor()
                    cursor.execute("set session interactive_timeout=%s" % self.max_timeout)
                    cursor.execute("set session wait_timeout=%s" % self.max_timeout)
                    cursor.close()
                return conn
            except Exception, e:
                # FIXME: the exception may be cursor.execute or cursor.close(), should handle them respectively
                retry_cnt -= 1
                if retry_cnt <= 0:
                    raise MySQLdb.Error("can't connect mysql for manny times: %s", str(e))

if __name__ == "__main__":

    params = {}
    params["host"] = "xxxxxx"
    params["port"] = 3303
    params["user"] = "xxxxxx"
    params["passwd"] = "xxxxxx"
    params["db"] = "mc_query_digest"
    params["charset"] = "utf8"
    dbc = Session(**params)
    sql = '''xxxxx'''
    print sql
    ret_iter = dbc.query(sql)
    for rec in ret_iter:
        print rec
