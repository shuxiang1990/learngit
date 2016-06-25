#!/usr/local/sinasrv2/bin/python2.7
# -*- coding: utf-8 -*-

#####################################################################
# option 1
# pros: common used
# cons: if the func need to return something, you should use a queue
#       to communicate with other process
#####################################################################
import multiprocessing
import functools
class TimeoutException(Exception):
    pass

def timeout_process(seconds):
  def decorated(func):
    def wrapper(*args, **kwargs):
      prs = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
      prs.start()
      prs.join(seconds)
      if prs.is_alive():
        prs.terminate()
        raise TimeoutException("timeout")
    return functools.wraps(func)(wrapper)
  return decorated

#####################################################################
# option 2
# pros: looks simple
# cons: 1: func must not be uninteruptable
#       2: must catch the exception or the caller thread/process will die
#       3: There is no return when func is timeout
#####################################################################
import sys
import signal, functools

def timeout_signal(seconds, error_message="Exception: <{0} seconds timeout>"):
    def decorated(func):
        def _handle_timeout(signum, frame):
            raise TimeoutException(error_message.format(seconds))
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                # func must not be uninteruptable
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
                return result
        return functools.wraps(func)(wrapper)
    return decorated

@timeout_process(3)
def slowfunc(sleep_time):
    a = 1
    import time
    time.sleep(sleep_time)
    return a

if __name__ == "__main__":
    print slowfunc(11)
