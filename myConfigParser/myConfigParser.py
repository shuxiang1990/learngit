#! /usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser

cp = ConfigParser.ConfigParser()

cp.read("test.conf")

for sec in cp.sections():
  print "sec: {0}".format(sec)
  print cp.items(sec)


print cp.get("sec_a", "a_key1")
print cp.getint("sec_a", "a_key2")


# write config
# update value
cp.set("sec_b", "b_key1", "new-$r")
# set a new value
cp.set("sec_b", "b_newkey", "new-value")
# create a new section
cp.add_section("a_new_section")
cp.set("a_new_section", "new_key", "new_value")

# write back to configure file
cp.write(open("test.conf", "w"))
