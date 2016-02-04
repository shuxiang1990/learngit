#! /usr/bin/python
# -*- coding: utf-8 -*-

import logging
from logging import handlers
import logging_child

# 如果改成logging.DEBUG，那么INFO，DEBUG信息都能打印出来
log_level = logging.INFO
log_filename = "logging.log"
# 获取名字为main的logger，默认名字是root
logger_m = logging.getLogger("main") 
logger_m.setLevel(log_level)
handler = handlers.RotatingFileHandler(log_filename, maxBytes=50000000, backupCount=0)
formatter = logging.Formatter("%(asctime)s - [%(levelname)s] - [%(filename)s: %(lineno)d] %(message)s")
handler.setFormatter(formatter)
logger_m.addHandler(handler)

logger_m.info("info_main")
logger_m.debug("debug_main")

logging_child.test()
