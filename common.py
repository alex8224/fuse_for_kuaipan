# -*-coding:utf-8 -*-
#****************************************************
# Author: tony - birdaccp@gmail.com
# Create by: 2013-08-17 11:01
# Last modified: 2013-08-17 11:01
# Filename: common.py
# Description:
#****************************************************
import signal
import sys
def handler(signum, frame):
    print "use press ctrl+c exit"
    sys.exit(0)
signal.signal(signal.SIGINT, handler)

import random
import string
import time
import logging
import logging.handlers
import base64
import hashlib
import hmac
import urllib

SIGNATURE_CHARACTERS = string.letters + string.digits + '_'

__api_log = "kuaipanapi.log"

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

def random_string():
    lens = random.randint(1,31)
    s = []
    for i in range(lens):
        s.append(random.choice(SIGNATURE_CHARACTERS))
    return ''.join(s)

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

if __name__ == "__main__":
    pass

