#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from db_api import Connection, MySQLdb
import time
import logging

FILTER_USER = ""
FILTER_CODE = (1146, 1396)
LOGGER = logging.getLogger(__name__)

class UserExistsError(Exception):
    message = "UserExistsError"
    http_code = 417

class Map(dict):
    def __init__(self, **entries):
        super(Map, self).__init__(entries)
    def __getattribute__(self, item):
        return self.get(item, None)

class MySQLInstance(object):
    """common operation for mysql
    """
    def __init__(self, host, database, user, password, charset="utf8", **kwargs):
        self.conn = Connection(host=host, db=database, user=user, passwd=password, charset=charset, **kwargs)
        self.user = user
        self.host = host
        self.password = password
        self.database = database
        self.charset = charset

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    def get_connection(self):
        return self.conn

    def test(self):
        return self.conn.query("SELECT 1")

    @property
    def version(self):
        return self.conn.get('SELECT @@version').get('@@version')

    @property
    def version_info(self):
        return tuple([int(v) for v in self.version.split('.', 3)[:3]])

    @property
    def variables(self):
        return self.get_all_mysql_variables()

    @property
    def slave_status(self):
        '''
        :return: Map object, which contains key,value attributes, 
                 which mapping to slave status
        '''
        s_status = self.show_slave_status()
        return Map(**s_status) if s_status else Map(**{})

    def slave_ok(self):
        k_vs = self.slave_status
        return len(k_vs) != 0 and k_vs.Slave_IO_Running == 'Yes' \
                              and k_vs.Slave_SQL_Running == 'Yes'

    @property
    def default_storage_engine(self):
        return self.conn.get('SELECT @@default_storage_engine').get('@@default_storage_engine')

    @property
    def heartbeat(self):
        sql = "SELECT ts FROM test.heartbeat WHERE id=1"
        rs = self.conn.get(sql)
        return rs.get("ts")

    def show_database(self):
        sql = "SHOW DATABASES"
        return [each["Database"] for each in self.conn.query(sql)]

    def get_schema_default_charset(self, schema_name):
        sql = "SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s"
        rs = self.conn.get(sql, schema_name)
        return rs.get('DEFAULT_CHARACTER_SET_NAME')

    def get_all_mysql_variables(self):
        rs = self.conn.query("show variables")
        return dict([(each_val["Variable_name"], each_val["Value"]) for each_val in rs if rs])

    def get_mysql_variables(self, var_name, scope="GLOBAL"):
        """
        get the value of mysql variables
        :param var_name: mysql variables name
        :param scope: GLOBAL/SESSION
        """
        sql = "SHOW {0} VARIABLES LIKE %s ".format(scope)
        rs = self.conn.query(sql, var_name)
        return dict([(each_val["Variable_name"], each_val["Value"]) for each_val in rs if rs])

    def is_read_only(self, scope="GLOBAL"):
        sql = "SHOW  %s VARIABLES LIKE 'read_only'" % (scope if scope else "")

        rs = self.conn.get(sql)
        if rs.get("Value").upper() == "ON":
            return True
        elif rs.get("Value").upper() == "OFF":
            return False

    def show_processlist(self):
        """
        :return:
            [{'Id': 622923,
            'User': root,
            'Host': '127.0.0.1:53530',
            'db': NULL,
            'Command': 'Query',
            'Time': 0,
            'State': 'init',
            'State': 'Reading from net',
            'Info': NULL,
            'Rows_sent': 0,
            'Rows_examined': 0,
            'Rows_read': 1},
            ]

        """

        sql = "SHOW FULL PROCESSLIST"
        return self.conn.query(sql)

    def set_mysql_variables(self, var_name, var_value, scope="GLOBAL"):
        """set the value of mysql variables"""
        sql = "SET {0} {1} = %s ".format(scope, var_name)
        return self.conn.execute(sql, var_value)

    def set_read_only(self, scope="GLOBAL"):
        """
        :param scope:
        :return: 0
        """
        return self.set_mysql_variables("read_only", "ON", scope)

    def set_read_write(self, scope='GLOBAL'):
        return self.set_mysql_variables("read_only", "OFF", scope)

    def show_slave_status(self):
        sql = "SHOW SLAVE STATUS"
        return self.conn.get(sql)

    def show_master_status(self):
        sql = "SHOW MASTER STATUS"
        return self.conn.get(sql)

    def get_mysql_account(self, user=None):
        if self.version_info < (5, 7, 6):
            sql = "SELECT User, Host, Password FROM mysql.user"
        else:
            sql = "SELECT User, Host, authentication_string FROM mysql.user"
        if user:
            sql += " WHERE User=%s"
            rs = self.conn.query(sql, user)
        else:
            rs = self.conn.query(sql)

        if self.version_info >= (5, 7, 6):
            [e.update({"Password": e["authentication_string"]}) for e in rs]

        return rs

    def get_user_and_password(self, user, host):
        if self.version_info < (5, 7, 6):
            sql = "SELECT User, Host, Password FROM mysql.user WHERE User=%s AND Host=%s"
        else:
            sql = "SELECT User, Host, authentication_string FROM mysql.user WHERE User=%s AND Host=%s"
        rs = self.conn.get(sql, user, host)
        if self.version_info >= (5, 7, 6):
            rs["Password"] = rs["authentication_string"]
        return '%s@%s:%s' % (user, host, rs['Password'])

    def get_all_users_and_privileges(self, filter_user=FILTER_USER):
        if isinstance(filter_user, (str, unicode)):
            filter_user = sorted(set(filter_user.split(',')))
        if filter_user:
            if self.version_info < (5, 7, 6):
                sql = 'SELECT User, Host, Password FROM mysql.user WHERE User NOT IN ({cond})'
            else:
                sql = "SELECT User, Host, authentication_string FROM mysql.user WHERE User NOT IN ({cond})"
            sql = sql.format(cond=','.join(["%s"] * len(filter_user)))
            rs = self.conn.query(sql, *filter_user)
        else:
            if self.version_info < (5, 7, 6):
                sql = "SELECT User, Host, Password FROM mysql.user"
            else:
                sql = "SELECT User, Host, authentication_string FROM mysql.user"
            rs = self.conn.query(sql)

        if self.version_info >= (5, 7, 6):
            [e.update({"Password": e["authentication_string"]}) for e in rs]

        res = {}
        for u in rs:
            if u["User"]:
                try:
                    p = self.show_user_privileges(u['User'], u['Host'])
                except MySQLdb.Error, e:
                    # fixme: There is no such grant defined for user 'phpwind' on host 'bbs2.mm.cnz.alimama.com'
                    LOGGER.warning("%s, user:%s, host:%s" % (str(e.args[1]), u['User'], u['Host']))
                    if e.args[0] == 1141:
                        continue
                    else:
                        raise
                k = '%s@%s:%s' % (u['User'], u['Host'], u['Password'])
                res.update({k: p})
        return res

    def create_user_and_grant_privileges(self, user, host, password, privs, is_plain=False,
                                         force=True, filter_code=FILTER_CODE):
        """
        :param is_plain： true/false是否明文
        """
        if self.is_account_exists(user, host):
            if not force:
                raise UserExistsError('%s@%s' % (user, host))
            else:
                self.drop_mysql_account(user, host)
        self.create_mysql_account(user, password, host, is_plain=is_plain)
        for each_priv in privs.split(';'):
            try:
                self.conn.execute(each_priv.replace('%', '%%'))
            except MySQLdb.Error, e:
                if e.args[0] in filter_code:
                    LOGGER.warning("%s, sql:%s" % (str(e), each_priv))
                else:
                    raise

    def is_account_exists(self, user, host=None):
        sql = "SELECT User, Host FROM mysql.user WHERE User=%s AND Host=%s"
        rs = self.conn.query(sql, user, host if host is not None else "%")
        return True if rs else False

    def drop_mysql_account(self, user, host=None):
        sql = "DROP USER %s@%s"
        return self.conn.execute(sql, user, host if host is not None else "%")

    def create_mysql_account(self, user, password="", host=None, is_plain=True):
        if password:
            sql = "CREATE USER %s@%s IDENTIFIED BY {pass_type} %s".format(pass_type="" if is_plain else "PASSWORD")
            return self.conn.execute(sql, user, host if host else "%", password)
        else:
            sql = "CREATE USER %s@%s"
            return self.conn.execute(sql, user, host if host else "%")

    def set_account_password(self, user, password, host=None, is_plain=True):
        sql = "SET PASSWORD FOR %s@%s = {0}".format("PASSWORD(%s)" if is_plain else "%s")
        return self.conn.execute(sql, user, host if host is not None else '%', password)

    def show_user_privileges(self, user, host=None):
        sql = "SHOW GRANTS FOR %s@%s"
        rs = self.conn.query(sql, user, host if host is not None else "%")
        return ";".join([each.values()[0] for each in rs])

    def grant_user_privileges(self, privs, user, host="%", password=None,
                              db_name="*", table_name="*", is_plain=True,
                              with_opts=False):
        sql = "GRANT {privs} ON {db_name}.{table_name} " \
              "TO %s@%s ".format(privs=privs,
                                 db_name=db_name if db_name else '*',
                                 table_name=table_name if table_name else '*')
        if password is not None:
            sql += " IDENTIFIED BY {key_word} %s".format(key_word="" if is_plain else "PASSWORD")
        if with_opts:
            sql += "WITH GRANT OPTION"
        if password is not None:
            return self.conn.execute(sql, user, host if host else '%', password)
        else:
            return self.conn.execute(sql, user, host if host else '%')

    def revoke_user_privilege(self, privs, user, host="%", db_name="*", table_name="*"):
        sql = "REVOKE {privs} ON {db_name}.{table_name} " \
              "FROM %s@%s".format(privs=privs, db_name=db_name, table_name=table_name)
        return self.conn.execute(sql, user, host)

    def get_user_session(self, user):
        return self.get_session(user=user)

    def get_session(self, db_name=None, user=None, filter_user=FILTER_USER, state=None):
        """
        :param db_name:
        :param user:
        :param filter_user:
        :param state
        :return:
        """
        if isinstance(filter_user, (unicode, str)):
            filter_user = [each.strip() for each in filter_user.split(",")]
        filter_user.append(self.user)

        if db_name:
            if isinstance(db_name, (unicode, str)):
                db_name = [each.strip() for each in db_name.split('|')]
        if user:
            if isinstance(user, (unicode, str)):
                user = [each.strip() for each in user.split(',')]

        if state:
            if isinstance(state, (unicode, str)):
                state = state.split(",")

        sql = "SHOW FULL PROCESSLIST"
        rs = self.conn.query(sql)
        if db_name:
            rs = [each for each in rs if each["db"] in db_name]
        if user:
            rs = [each for each in rs if each["User"] in user]
        if state:
            rs = [each for each in rs if each["State"] in state]
        return [each["Id"] for each in rs if each["User"] not in filter_user]

    def kill_session(self, session_id):
        if isinstance(session_id, (str, unicode)):
            session_id = session_id.split(",")
        if session_id:
            kill_sql = "KILL %s"
            session_args = [(each_id, ) for each_id in session_id]
            return self.conn.executemany(kill_sql, session_args)
        return 0

    def kill_session_by_user(self, user=None, filter_user=FILTER_USER, retry_times=16):
        """
        :param user:
        :param filter_user:
        :param retry_times
        :return:
        """
        for idx in range(retry_times):
            session_list = self.get_session(user=user, filter_user=filter_user)
            if session_list:
                self.kill_session(session_list)
                time.sleep(0.1)
            else:
                break
        return 0

    def kill_session_by_db(self, db_list=None, filter_user=FILTER_USER, retry_times=30):
        """
        :param db_list:
        :param filter_user:
        :param retry_times
        :return:
        """
        for idx in range(retry_times):
            session_list = self.get_session(db_name=db_list, filter_user=filter_user)
            if session_list:
                self.kill_session(session_list)
                time.sleep(0.1)
            else:
                break
        return 0

    def change_master_to(self, host, port, user, password, file_name=None, pos=None):
        stop_slave = "STOP SLAVE"
        self.conn.execute(stop_slave)
        change_sql = "CHANGE MASTER TO MASTER_HOST=%s, MASTER_PORT=%s, MASTER_USER=%s, " \
                     "MASTER_PASSWORD=%s, MASTER_LOG_FILE=%s, MASTER_LOG_POS=%s"
        # gtid_sql = "CHANGE MASTER TO MASTER_HOST=%s, MASTER_PORT=%s, MASTER_USER=%s, " \
        #            "MASTER_PASSWORD=%s, MASTER_AUTO_POSITION=1"
        try:
            # fixme: add support for gtid, query if the gtid mode is on
            # gtid = self.get_mysql_variables('gtid_mode')
            # if gtid and gtid["gtid_mode"] == "ON":
            #     self.conn.execute(gtid_sql, host, int(port), user, password)
            # else:
            #     if not file_name and not pos:
            #         target_conn = DBHandler("%s:%s" % (host, port), self.database, self.user,
            #                                 self.password, self.charset)
            #         m_status = target_conn.show_master_status()
            #         file_name = m_status["File"]
            #         pos = m_status["Position"]
            #     self.conn.execute(change_sql, host, int(port), user, password, file_name, int(pos))
            if not file_name and not pos:
                target_conn = DBHandler("%s:%s" % (host, port), self.database, self.user,
                                        self.password, self.charset)
                m_status = target_conn.show_master_status()
                file_name = m_status["File"]
                pos = m_status["Position"]
            self.conn.execute(change_sql, host, int(port), user, password, file_name, int(pos))
            return 0
        finally:
            start_slave = "START SLAVE"
            self.conn.execute(start_slave)

    def stop_slave(self):
        sql = "STOP SLAVE"
        return self.conn.execute(sql)

    def reset_slave(self, is_all=False):
        sql = "RESET SLAVE ALL" if is_all else "RESET SLAVE"
        return self.conn.execute(sql)

    def reset_master(self):
        sql = "RESET MASTER"
        return self.conn.execute(sql)

    def start_slave(self):
        sql = "START SLAVE"
        return self.conn.execute(sql)

    def start_slave_until_pos(self, master_file, master_pos):
        sql = "START SLAVE UNTIL MASTER_LOG_FILE=%s, MASTER_LOG_POS=%s"
        return self.conn.execute(sql, master_file, master_pos)

    def create_database(self, db_name, charset="utf8"):
        sql = "CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET %s" % (db_name, charset)
        return self.conn.execute(sql)

    def drop_database(self, db_name):
        sql = "DROP DATABASE IF EXISTS %s" % db_name
        return self.conn.execute(sql)

    def rename_table(self, old_name, new_name, new_db_name=None):
        sql = "ALTER TABLE {0} RENAME TO {1}{2}".format(old_name, new_db_name+'.' if new_db_name else "", new_name)
        return self.conn.execute(sql)

    def update_heartbeat(self):
        sql = "INSERT INTO test.heartbeat(id, ts) VALUES (1, UNIX_TIMESTAMP()) " \
              "ON DUPLICATE KEY UPDATE ts=UNIX_TIMESTAMP()"
        return self.conn.execute(sql)

    def copy_table(self, src_tb, dst_tb, sql_mode=True, force=False, nodata=True):
        """
        :param sql_mode: if true: use create table statement, else use create table like statement
        :param force:
        :param nodata: copy structure but not data
        :return:
        """
        sql = "SHOW TABLES LIKE %s"
        exist_tb = self.conn.query(sql, dst_tb)
        if exist_tb:
            if not force:
                raise TableExistsError(dst_tb)
            else:
                sql = "DROP TABLE %s" % dst_tb
                self.conn.execute(sql)

        if sql_mode:
            rs = self.conn.get("SHOW CREATE TABLE %s" % src_tb)
            sql = rs["Create Table"].lower().replace(src_tb.lower(), dst_tb.lower())
        else:
            sql = "CREATE TABLE %s LIKE %s" % (dst_tb, src_tb)
        self.conn.execute(sql)
        if not nodata:
            sql = "INSERT INTO %s SELECT * FROM %s" % (dst_tb, src_tb)
            self.conn.execute(sql)

    def exchange_table(self, src_tb, dst_tb):
        tmp_tb = '%s__tmp' % src_tb
        sql = "RENAME TABLE %s to %s, %s to %s, %s to %s" % (src_tb, tmp_tb, dst_tb, src_tb, tmp_tb, dst_tb)
        return self.conn.execute(sql)

    def drop_table(self, tb_name):
        sql = "SHOW TABLES LIKE %s"
        if self.conn.get(sql, tb_name):
            self.conn.execute('DROP TABLE %s' % tb_name)

    def get_binlog_list(self):
        sql = "SHOW BINARY LOGS"
        return self.conn.query(sql)

    def get_binlog_events(self, log_file, start=0, count=1):
        sql = "SHOW BINLOG EVENTS IN %s LIMIT %s, %s"
        return self.conn.query(sql, log_file, start, count)

    def get_binlog_content(self, log_file, start_pos=4, end_pos=120):
        from .utils import execute_shell
        ip, port = self.host.split(':')
        cmd = "mysqlbinlog --no-defaults -h%s -P%s -u%s -p%s --read-from-remote-server %s " \
              "--start-position=%s --stop-position=%s" % (ip, port, self.user, self.password,
                                                          log_file, start_pos, end_pos)
        return execute_shell(cmd)

    @property
    def binlog_start_time(self):
        import re
        re_cmp = re.compile('^#(\d+\s+\d+:\d+:\d+)')
        binlog_list = self.get_binlog_list()
        first_log_file = binlog_list[0].get('Log_name')
        first_event = self.get_binlog_events(first_log_file)
        log_content = self.get_binlog_content(first_log_file, first_event[0].get('Pos'),
                                              first_event[0].get('End_log_pos'))
        for line in log_content.split('\n'):
            re_mth = re_cmp.match(line)
            if re_mth:
                return time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(re_mth.group(1), '%y%m%d %H:%M:%S'))
        return None

    @property
    def super_privileges(self):
        def verify_super(privs):
            return True if ('SUPER' in privs or 'ALL PRIVILEGES' in privs) else False
        import socket
        ip = socket.gethostbyname(socket.gethostname())
        if self.is_account_exists(self.user, ip):
            return verify_super(self.show_user_privileges(self.user, ip))
        elif self.is_account_exists(self.user):
            return verify_super(self.show_user_privileges(self.user))
        return False

    def revoke_schema_privileges(self, schema, privileges="SELECT,INSERT,UPDATE,DELETE", filter_user=FILTER_USER):
        import re
        re_cmp = re.compile('^GRANT\s+([\w,\s]+)\s+ON\s+((.+)\.(.+))\s+TO', re.I)
        revoke_list = []
        user_info = self.get_all_users_and_privileges(filter_user)
        for u, privs in user_info.iteritems():
            uh, _ = u.split(":", 1)
            user, host = uh.split("@")
            for l in privs.split(";"):
                re_mth = re_cmp.match(l.strip())
                if re_mth:
                    db = re_mth.group(3).strip('`')
                    if db in schema:
                        priv = (','.join([b for b in re_mth.group(1).split(",") if b.strip() in privileges])) \
                            if privileges else re_mth.group(1)
                        if priv:
                            revoke_list.append((priv, re_mth.group(2), user, host))
        if revoke_list:
            for p, s, u, h in revoke_list:
                sql = "REVOKE {privs} ON {scope} FROM %s@%s".format(privs=p, scope=s)
                self.conn.execute(sql, u, h)

class LocalMySQLInstance(MySQLInstance):
    def __init__(self, port, user="root", password="", database="", charset="utf8", **kwargs):
        super(LocalMySQLInstance, self).__init__("%s:%s" % ("127.0.0.1", port) if str(port).isdigit() else port,
                                             database=database,
                                             user=user,
                                             password=password,
                                             charset=charset, **kwargs)

if __name__ == "__main__":
    lh = LocalMySQLInstance(3306)
