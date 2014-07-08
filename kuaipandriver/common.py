# -*-coding:utf-8 -*-
#****************************************************
# Author: tony - birdaccp@gmail.com alex8224@gmail.com
# Create by: 2013-08-17 11:01
# Last modified: 2014-07-08 11:45
# Filename: common.py
# Description:
#****************************************************
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
    # from threading import RLock
    # lock = RLock()
    _counter = AtomCounter()
    _result = _counter()
    def getnext():
        # with lock:
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

def logger():
    return __setlogger(__api_log)


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
