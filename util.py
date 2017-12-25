#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

import commands
import signal

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
