# -*-coding:utf-8 -*-
#****************************************************
# Author: tony - birdaccp@gmail.com alex8224@gmail.com
# Create by: 2013-08-17 11:01
# Last modified: 2014-07-08 11:45
# Filename: common.py
# Description:
#****************************************************

import os
from os.path import expanduser
import hmac
import time
import base64
import urllib
import logging
import hashlib
import logging.handlers

__api_log = "kuaipanapi.log"

def AtomCounter():

    def incrcounter():
        while 1:
            timestamp = str(long(time.time() * 1000000))
            yield timestamp
    return incrcounter

def oauth_once_next():
    _counter = AtomCounter()
    _result = _counter()
    def getnext():
        count = _result.next()
        return count

    return getnext


def __setlogger(logfile):
    logger = logging.getLogger()
    rh = logging.handlers.TimedRotatingFileHandler(logfile, 'D', backupCount=30)
    fm = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    rh.setFormatter(fm)
    logger.addHandler(rh)
    logger.setLevel(logging.NOTSET)
    return logger

# def logger():
    # return __setlogger(__api_log)


def to_timestamp(timestr):
    struct_time = time.strptime(timestr,"%Y-%m-%d %H:%M:%S")
    return int(time.mktime(struct_time))


def to_string(a):
    if type(a) is bool:
        s = 'true' if a else 'false'
    elif type(a) is unicode:
        s = a.encode('utf-8')
    else:
        s = str(a)
    return s

def quote(s):
    s = to_string(s)
    return urllib.quote(s, '~')

def generate_signature(base_uri, parameters, key, http_method='get'):
    s = ''
    s += (http_method.upper()+'&')
    s += (quote(base_uri) + '&')
    s += quote('&'.join(sorted([quote(k) + "=" + quote(v) for k, v in parameters.items()])))
    
    s = hmac.new(key, s, hashlib.sha1).digest()
    s = base64.b64encode(s)
    s = quote(s)
    return s

def timestamp():
    return int(time.time())

def get_request_url(base_url, parameters, signature):
    s = base_url + '?'
    s += '&'.join([to_string(k) + "=" + quote(v) for k,v in parameters.items()])
    s += '&oauth_signature=' + signature
    return s

def safe_value(v):
    if type(v) is unicode:
        return v.encode('utf-8')
    else:
        return v

def getauthinfo():
    '''get login information from local disk or stdin'''
    from kuaipandriver.common import getloginfromlocal
    loginfo = getloginfromlocal()
    if loginfo:
        return loginfo

    mntpoint = raw_input("MountPoint(Absoluate Path):")
    key = raw_input("ConsumerKey:")
    secret = raw_input("ConsumerSecret:")
    username = raw_input("Kuaipan Login Name:")
    from getpass import getpass
    pwd = getpass("Kuaipan Password:")
    return mntpoint, key, secret, username, pwd

def getloginfromlocal():
    '''get login information from local disk'''
    infofilepath= os.path.expanduser("~") + "/.kuaipandriver"
    if os.path.exists(infofilepath):
        import pickle
        return pickle.load(file(infofilepath))

def savelogin(mntpoint, key, secret, username, pwd):
    '''save login information to home directory ~/.kuaipandriver'''
    import os
    infofilepath = expanduser("~") + "/.kuaipandriver"
    if os.path.exists(infofilepath):
        return
    save = raw_input("Do you want save your login info?(Y/N)")
    if save == "Y":
        infofilepath = os.path.expanduser("~")+ "/.kuaipandriver"
        with open(infofilepath, "w") as infofile:
            info = (mntpoint, key, secret, username, pwd )
            import pickle
            pickle.dump(info, infofile)
            print("your login info saved in path %s" % infofilepath)


def deleteloginfo():
    infofilepath = expanduser("~")+ "/.kuaipandriver"
    try:
        os.unlink(infofilepath)
    except:
        pass

def checkplatform():
    import sys
    platform = sys.platform
    if not platform.startswith("linux"):
        sys.stderr.write("Kuaipandriver only support Linux Platform")
        sys.exit(1)


if __name__ == '__main__':
    checkplatform()        

