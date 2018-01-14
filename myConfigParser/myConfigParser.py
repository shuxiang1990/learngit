#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ConfigParser

class Config(object):

    def __init__(self, cfg_file):

        self.cfg = ConfigParser.ConfigParser()
        self.cfg.read(cfg_file)
        
    @property
    def zk_hosts(self):
        return self.cfg.get("zk_cfg", "hosts")

    @property
    def zk_port(self):
        return self.cfg.get("zk_cfg", "port")

    @property
    def zk_schema(self):
        return self.cfg.get("zk_cfg", "schema")

    @property
    def zk_credential(self):
        return self.cfg.get("zk_cfg", "credential")

    @property
    def zk_lock_path(self):
        return self.cfg.get("zk_cfg", "lock_path")

    @property
    def zk_node_value(self):
        return self.cfg.get("zk_cfg", "zk_node_value")

    @property
    def dc_3304_host(self):
        return self.cfg.get("datacheck_3304_cfg", "host")

    @property
    def dc_3304_port(self):
        return self.cfg.get("datacheck_3304_cfg", "port")

    @property
    def dc_3304_user(self):
        return self.cfg.get("datacheck_3304_cfg", "user")

    @property
    def dc_3304_passwd(self):
        return self.cfg.get("datacheck_3304_cfg", "passwd")

    @property
    def dc_3304_db(self):
        return self.cfg.get("datacheck_3304_cfg", "db")

    @property
    def dc_3304_charset(self):
        return self.cfg.get("datacheck_3304_cfg", "charset")


    @property
    def pt_hosts(self):
        return self.cfg.get("pt_sync_cfg", "hosts")

    @property
    def pt_passwd(self):
        return self.cfg.get("pt_sync_cfg", "passwd")

    @property
    def pt_user(self):
        return self.cfg.get("pt_sync_cfg", "user")

    @property
    def super_db(self):
        return self.cfg.get("super_grant_cfg", "db")

    @property
    def super_user(self):
        return self.cfg.get("super_grant_cfg", "user")

    @property
    def super_passwd(self):
        return self.cfg.get("super_grant_cfg", "passwd")

    @property
    def datacheck_3304_cfg(self):
        return dict(self.cfg.items("datacheck_3304_cfg"))

    @property
    def zk_cfg(self):
        return dict(self.cfg.items("zk_cfg"))

    @property
    def pt_sync_cfg(self):
        return dict(self.cfg.items("pt_sync_cfg"))

    @property
    def super_grant_cfg(self):
        return dict(self.cfg.items("super_grant_cfg"))

    @property
    def thread_pool_cfg(self):
        return dict(self.cfg.items("thread_pool_cfg"))

    @property
    def sections(self):
        return self.cfg.sections()

    def options(self, cfg_sec_name):
        return self.cfg.options(cfg_sec_name)

CONF = Config("wenger.conf")
