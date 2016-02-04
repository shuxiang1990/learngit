#! /bin/bash
# -*- coding: utf-8 -*-

import logging


#logger名字按层级命名，子模块的logger可以继承父模块的logger配置
name = "main" + r"." + __name__  
logger = logging.getLogger(name)

def test():
  logger.info("info_child")
  logger.debug("debug_child")
