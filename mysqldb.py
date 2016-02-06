# coding: utf-8

"""A lightweight wrapper around MySQLdb."""
import copy
import MySQLdb
import MySQLdb.constants
import MySQLdb.converters
import MySQLdb.cursors
import itertools
import logging
from time import time

# Alias some common MySQL exceptions
IntegrityError = MySQLdb.IntegrityError
OperationalError = MySQLdb.OperationalError

class Connection(object):
    """
    A lightweight wrapper around MySQLdb DB-API connections.

    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage:

        db = database.Connection("localhost", "mydatabase")
        for row in db.raw_query("SELECT * FROM articles"):
            print row

    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the DB-API.

    WARNING: ***** NOT THREAD SAFE *****

    """

    def __init__(self, host="127.0.0.1", port=3306, db="test", user=None, passwd=None, charset="utf8", connect_timeout=5):
        '''
        '''
        self.args = {}
        self.args["db"] = db
        self.args["charset"] = charset
        self.args["user"] = user
        self.args["passwd"] = passwd
        self.args["host"] = host
        self.args["port"] = port
        self.args["connect_timeout"] = connect_timeout
        self._db = None # connection

    def raw_query(self, query):
        """Returns a row list for the given query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query)
            return [row for row in cursor]
        finally:
            cursor.close()

    def raw_execute(self, query):
        """Executes the given query, including insert, update and delete."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query)
        finally:
            cursor.close()

    def executemany(self, query):
        """Executes many queries"""
        cursor = self._cursor()
        try:
            cursor.executemany(query)
        finally:
            cursor.close()

    def close(self):
        """Closes this database connection."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def commit(self):
        if self._db is not None:
            try:
                self._db.ping()
            except:
                self.reconnect()
            try:
                self._db.commit()
            except Exception,e:
                logging.debug("Can not commit",e)
                self._db.rollback()

    def rollback(self):
        if self._db is not None:
            try:
                self._db.rollback()
            except Exception,e:
                logging.debug("Can not rollback")

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = MySQLdb.connect(**self.args)
        self._db.autocommit(False)


    def iter(self, query):
        """Returns an iterator for the given query."""
        if self._db is None: self.reconnect()
        cursor = MySQLdb.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query)
            for row in cursor:
                yield row
        finally:
            cursor.close()

    def _cursor(self):
        if self._db is None:
            self.reconnect()
        try:
            self._db.ping()
        except:
            self.reconnect()
        return self._db.cursor()

    def _execute(self, cursor, query):
        try:
            cursor.execute(query)
            self.commit()
        except OperationalError:
            logging.error("Error connecting to MySQL on %s", self.args["host"])
            self.close()
            raise

    def __del__(self):
        self.close()

if __name__ == "__main__":
    db = Connection(host="127.0.0.1", port=3309, db="test", user="root", passwd="123456") 
    print db.raw_query("select * from test2")
#    print db.raw_execute('insert into test2 values ("6", "test")')
#    print db.raw_execute('update test2 set a = "7" where a = "6"')
    print db.raw_execute('delete from test2 where a = "7"')
