#!/usr/bin/python
# -*-coding:utf-8 -*-
#****************************************************
# Author: tony - birdaccp@gmail.com
# Create by: 2013-08-17 11:06
# Last modified: 2013-08-17 11:06
# Filename: config.py
# Description:
#****************************************************
import common
import traceback
from ConfigParser import ConfigParser
from pkg_resources import resource_filename
defaultNode = 'DEFAULT'
configName = resource_filename("kuaipandriver", "config.ini")

class _Method:
    def __init__(self, configobj, name):
        self.configobj = configobj
        self.name = name

    def __call__(self, *args):
        name = self.name[4:]
        return str(self.configobj[name])

class Config(object):
    def __init__(self):
        try:
            cf = ConfigParser()
            cf.read(configName)
            self.configobj = dict(cf.items(defaultNode))
        except Exception:
            common.logger().error(" read config error: %s" % traceback.format_exc())

    def __getattr__(self, name):
        return _Method(self.configobj, name)


if __name__ == "__main__":
    config = Config()
    print config.get_kuaipan_api_url_root()
    print config.get_fileops_base_url()
    print config.abc_fileops_copy_base_url()

