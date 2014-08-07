# -*-coding:utf-8 -*-
#****************************************************
# Author: tony - birdaccp@gmail.com alex8224@gmail.com
# Create by: 2013-08-17 11:01
# Last modified: 2014-07-08 11:45
# Filename: common.py
# Description:
# some common functions
#****************************************************

import os
import hmac
import time
import base64
import hashlib
import traceback
from copy import copy
from threading import RLock
from os.path import expanduser
from ConfigParser import ConfigParser
from pkg_resources import resource_filename
from urllib import quote as uquote


class Meta(type):
    def __new__(cls, name, bases, attrs):
        from threading import Lock
        attrs["_global_lock"] = Lock()
        return super(Meta, cls).__new__(cls, name, bases, attrs)

    def __init__(clsa, name, bases, attrs):
        super(Meta, clsa).__init__(name, bases, attrs)

    def instance(clas, *args, **kwargs):
        with clas._global_lock:
            if not hasattr(clas, "_instance"):
                clas._instance = super(Meta, clas).__call__(*args, **kwargs)
            return clas._instance    

class Singleton(object): __metaclass__ = Meta

class Context(Singleton):pass

def define(**kwargs):
    context = Context.instance()
    for k,v in kwargs.iteritems():
        setattr(context, k, v)

def getlogger(logfile=None, level="DEBUG"):
    import logging
    from logging import handlers
    levels = {"INFO":logging.INFO, "DEBUG":logging.DEBUG, "WARN":logging.WARN,"ERROR":logging.ERROR, "FATAL":logging.FATAL}
    logger = logging.getLogger("kuaipanserver")
    format = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    hdlr = logging.StreamHandler()
    logger.addHandler(hdlr)
    hdlr.setFormatter(format)
    if logfile:
        fdlr = handlers.TimedRotatingFileHandler(logfile, 'D', backupCount=30)
        logger.addHandler(fdlr)
        fdlr.setFormatter(format)
    logger.setLevel(levels[level])
    return logger

logger = getlogger()

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
            defaultNode = 'DEFAULT'
            configName = resource_filename("kuaipandriver", "config.ini")
            cf = ConfigParser()
            cf.read(configName)
            self.configobj = dict(cf.items(defaultNode))
        except Exception:
            print(" read config error: %s" % traceback.format_exc())

    def __getattr__(self, name):
        return _Method(self.configobj, name)

config = Config()

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
    return uquote(s, '~')

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

def checkplatform():
    import sys
    platform = sys.platform
    if not platform.startswith("linux"):
        sys.stderr.write("Kuaipandriver only support Linux Platform")
        sys.exit(1)

def gethomedir():
    return expanduser("~")

class CopyOnWriteBuffer(object):

    def __init__(self):
        self.buff = ''
        self.buflist = []
        self.lock = RLock()
        self.readindex = 0
        self._length = 0

    def read(self, n=-1):
        if n == -1:
            return self.buff[self.readindex:]
        else:
            return self.buff[self.readindex:self.readindex+n]

    def write(self, chunk):
           self.buflist.append(chunk)
           self._length += len(chunk)
           newbuff = copy(self.buff)
           newbuff += "".join(self.buflist)
           self.buflist = []
           with self.lock:
               self.buff = newbuff

    def seek(self, offset):
        assert offset < self.length, "offset cannot greater than length"
        self.readindex = offset

    def clear(self):
        with self.lock:
            self.buff = ''
            self.readindex = 0
            self.buflist = []
            self.length = 0

    @property
    def length(self):
        with self.lock:
            return self._length

    def tell(self):
        return self.readindex

class CacheableObject(object):
    def __init__(self):
        self._key= ''
        self._value = ''
        self._hitcount = 0

    @property
    def hitcount(self):
       return self._hitcount

    @hitcount.setter
    def hitcount(self, count):
        self._hitcount = count

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key):
        self._key = key

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def __str__(self):
        return "[key=%s, hitcount=%d]" % (self._key, self._hitcount)


class DiskCacheable(CacheableObject):

    def __init__(self):
        CacheableObject.__init__(self)
        self.cachedir = config.get_cache_object_dir()

    @property
    def value(self):
        return file(self.cachedir + self.key)

    @value.setter
    def value(self, filedata):
        with open(self.cachedir + self.key, "w") as cachefile:
            cachefile.write(filedata)

    def destroy(self):
        try:
            os.unlink(self.cachedir + self.key)
        except:
            print("delete cachefile failed!")

class LRUCache(object):
    '''
    {"key": {"ttl": seconds, "object": cacheableobj}
    '''
    def __init__(self):
        self.cap = config.get_cache_object_number()
        self.cache = {}
        self.lruconfig = expanduser("~") + os.sep + ".lru.db"

    def set(self, key, value):
        '''save object to cache'''
        if len(self.cache) == self.cap:
            needeliminate = self.findelimination()
            if needeliminate:
                key = needeliminate.key
                self.remove(key)

        self.cache[key] = {}
        if key in self.cache:
            self.cache[key]["hitcount"] = 0

        self.cache[key]["ttl"] = 3600
        self.cache[key]["object"] = value

    def findelimination(self):
        lastcacheobj = None
        for cacheobj in self.cache.itervalues():
            cacheobj = cacheobj["object"]
            if lastcacheobj:
                if cacheobj.hitcount < lastcacheobj.hitcount:
                    lastcacheobj = cacheobj
            else:
                lastcacheobj = cacheobj

        return lastcacheobj        

    def remove(self, key):
        if key in self.cache:
            self.cache[key]["object"].destroy()
            del self.cache[key]

    def get(self, key):
        if key in self.cache:
            cacheobj = self.cache[key]["object"]
            cacheobj.hitcount = cacheobj.hitcount+1
            return cacheobj

    @property
    def count(self):
        return len(self.cache)

    def save(self):
        import cPickle as pickle
        pickle.dump(self.cache, open(self.lruconfig, "w"))

    def load(self):
        if os.path.exists(self.lruconfig):
            import cPickle as pickle
            self.cache = pickle.load(file(self.lruconfig))

class SafeLRUCache(Singleton, LRUCache):
    
    def __init__(self):
        super(SafeLRUCache, self).__init__()
        self.lock = RLock()

    def set(self, key, value):
        with self.lock:
            super(SafeLRUCache, self).set(key, value)

    def get(self, key):
        with self.lock:
            return super(SafeLRUCache, self).get(key)

    @property
    def count(self):
        with self.lock:
            return super(SafeLRUCache, self).count()

if __name__ == '__main__':
    from kuaipanfuse import ThreadPool
    from httputils import httpget
    ThreadPool.instance().start()
    

    # asyncrequest = BatchRequest()
    urls = ["http://localhost/phpmyadmin/index.php" for url in range(1000)]
    tasks = []
    for url in urls:
        f = ThreadPool.instance().submit(httpget, url)
        tasks.append(f)
    allstatus = []
    for task in tasks:
        r = f.get()
        allstatus.append(r)
        
    ThreadPool.instance().quit()
    print len(filter(lambda r:r.status_code == 200, allstatus))

    # hs = []
    # for url in urls:
        # hs.append(asyncrequest.get(url))
    # 
    # asyncrequest.run()    
    # num_handles, oks, nooks = asyncrequest.info_read()
    # print num_handles, len(oks)
    # okhandler = filter(lambda r:r.status_code == 200, asyncrequest.get_result())
    # failhandler = filter(lambda r:r.status_code != 200, asyncrequest.get_result())
    # print len(okhandler), len(failhandler)
    # print failhandler
