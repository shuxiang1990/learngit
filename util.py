#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import traceback

import commands
import signal
import functools

def exec_shell_local(shell_cmd):
    '''exec shell command, returns results as shell does
        Returns
            (-1,e) when exception, (status, output) when executed
    '''
    try:
        status, output = commands.getstatusoutput(shell_cmd)
    except Exception, e:
        logger.error(e)
        return (-1, e)
    return (status, output)

class op_signal(object):
    '''python 程序里处理外部信号的简单例子，主要针对 unix 系统
       **WARNING**
       在多线程环境下对于同一个信号如果当前一个信号还没处理完接着又来一个，
       那么后面来的信号会被丢弃，这里需要注意
    '''
    @staticmethod
    def register_signal(sig_and_handler_map):
        '''for example: {signal.SIGHUP:sig_hup_handler,signal.SIGINT:sig_int_handler,
                         signal.SIGTERM:sig_term_handler,signal.SIGQUIT:sig_quit_handler,
                         signal.SIGABRT:sig_abrt_handler,signal.SIGPIPE:sig_pipe_handler}
           is called sig_and_handler_map
        '''
        for sig, handler in sig_and_handler_map.iteritems():
            signal.signal(sig, handler)
    @staticmethod
    def tick(interval=5):
        '''like time clock, one tick per 5s
        '''
        def signal_handler(signum, frame):
            print 'Received signal: %d'%signum
        while True:
            signal.signal(signal.SIGALRM, signal_handler)
            signal.alarm(5)
            while True:
               print('waiting')
               time.sleep(1)
         
def retry(count=5, interval=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(count):
                rs = func(*args, **kwargs)
                if rs is not False and rs is not None:
                   return rs
                else:
                   time.sleep(interval)
            return rs
        return wrapper
    return decorator

def get_proper_file(dir, strategys, regx):
    '''For example:
        dir = "/u01/my3306/data"
        strategys = ["-mmin -60 -size +1G -size -3G", "-mmin -60 -size -3G", "-mmin -300 -size -5G", "-mmin -4320 -size -5G"]
        regx = \"*.ibd\"
    
        Returns:
            files/None
    '''
    files = None
    cmd = ("find %s -type f -name %s %s | grep -v test "
           "| grep -v mysql | grep -v information_schema | grep -v performance_schema "
           "| grep -v sys | grep -v recycle_bin")
    for i in range(len(strategys)):
        time.sleep(1)
        stra = strategys[i]
        real_cmd = cmd%(dir, regx, stra)
        status, output = exec_shell_local(real_cmd)
        if status == 0 and output:
            files = output
    return files

def get_slave_delay(port):
    ''' fetch slave delay, depends on heartbeat table but not SBM
    '''
    sql = """ select TO_SECONDS(now()) - TO_SECONDS(FROM_UNIXTIME(ts)) as diff_secs  from test.heartbeat where id=1 """
    rs = select_dict_rs('127.0.0.1', port, sql, user="root",passwd='', db="mysql")
    if not rs:
        logger.error("%s query heartbeat error"%port)
        return None
    else:
        diff_secs = rs[0]["diff_secs"]
        return int(diff_secs)

def wait_slave_delay(port, dt=259200, delay=5, dbtype="mysql"):
    '''wait until slave catch up, default time 3 days
    '''
    retry = 0
    count = dt/10
    # used for get_slave_delay exception retry
    exception_retry = 0
    while count and retry < 2:
        st = get_slave_delay(port)
        if not st:
            count -= 1
            time.sleep(10)
            continue
        logger.info("retry left: %d, slave delay: %d"%(count, st))
        if st <= delay:
            retry += 1
            count -= 1
            time.sleep(2)
            continue
        else:
            count -= 1
        time.sleep(10)
    if count == 0:
        logger.error("wait slave delay over 3 days, mark failed")
        return False
    return True

def utf8(unicode_str):
    if isinstance(unicode_str, unicode):
        unicode_str = unicode_str.encode('utf8')
    else:
        if not isinstance(unicode_str, str):
            unicode_str = str(unicode_str)
        unicode_str = unicode(unicode_str, "utf8", errors="ignore")
    return unicode_str
