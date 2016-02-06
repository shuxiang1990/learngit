#! /bin/bash
# -*- coding: utf-8 -*-

import sys

#---------- for getopt ------------
import getopt

try:
  options, args = getopt.getopt(sys.argv[1:], "hp:i:", ["help", "ip=", "port="])
except getopt.GetoptError:
  sys.exit()

print "============"
print options
print args
print "============"

# simple usage, should be modified and meet your own need
for name, value in options:
  if name in ("-h", "--help"):
    #usage()
    print "this is help option"
  elif name in ("-i", "--ip"):
    print "ip is {0}".format(value)
  elif name in ("-p", "--port"):
    print "port is {0}".format(value)
  else:
    pass

#------------ optparser ----------

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename", help="write report to FILE", metavar="FILE")
