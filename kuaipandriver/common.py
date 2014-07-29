# -*-coding:utf-8 -*-
#****************************************************
# Author: tony - birdaccp@gmail.com alex8224@gmail.com
# Create by: 2013-08-17 11:01
# Last modified: 2014-07-08 11:45
# Filename: common.py
# Description:
#****************************************************

import os
import hmac
import time
import pycurl
import base64
import urllib
import hashlib
import traceback
from copy import copy
from threading import RLock
from os.path import expanduser

from ConfigParser import ConfigParser
from pkg_resources import resource_filename

defaultNode = 'DEFAULT'
configName = resource_filename("kuaipandriver", "config.ini")


def getlogger():
    import logging
    from logging import handlers
    logger = logging.getLogger("kuaipanserver")
    hdlr = logging.StreamHandler()
    fdlr = handlers.TimedRotatingFileHandler("kuaipan_fuse.log", 'D', backupCount=30)
    format = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    logger.addHandler(hdlr)
    logger.addHandler(fdlr)
    hdlr.setFormatter(format)
    fdlr.setFormatter(format)
    logger.setLevel(logging.DEBUG)
    return logger


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
        with self.lock:
           self.buflist.append(chunk)
           self._length += len(chunk)
           newbuff = copy(self.buff)
           newbuff += "".join(self.buflist)
           self.buflist = []
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


class response(object):

    def __init__(self):
        self.headers = {}
        self._status_code = -1
        self._cachefile = CopyOnWriteBuffer()
        self._headers = {}
        self._body = None

    def __str__(self):
        return "[Response %d]" % self.status_code

    @property
    def status_code(self):
        return self._status_code

    @status_code.setter
    def status_code(self, status_code):
        self._status_code = status_code

    @property
    def cachefile(self):
        return self._cachefile

    @property
    def raw(self):
        return self._cachefile

    @raw.setter
    def raw(self, cachefile):
        self._cachefile = cachefile
 
    @property
    def text(self):
        return self._cachefile.read()

    def json(self):
        import json
        return json.loads(self._cachefile.read()) 


class httprequest(object):
    def __init__(self, curl=None, cookiefile=None, callback=None, cachefile=None):
        self.curl = curl if curl else pycurl.Curl()
        self.cookiefile = cookiefile if cookiefile else ''
        self.response = response()
        self.writecallback = callback
        self.cachefile = cachefile

    def _header2curlstyle(self, headers):
        return map(lambda h:(h[0] +": " + h[1]), headers.iteritems())
    
    def _dict2urlfields(self, payfields):
        return "&".join(["%s=%s" % (item[0],item[1]) for item in payfields.iteritems()])

    def parserstatus(self, statusline):
        s1 = statusline.find(" ")
        schema, self.response.status_code = statusline[0:s1], int(statusline[s1+1:s1+4])

    def parseheader(self, header):
        header = header.strip()
        firstsep = header.find(":")
        if firstsep>-1:
            headername = header[0:firstsep]
            headervalue = header[firstsep+2:]
            self.response.headers[headername] = headervalue


    def headerfunc(self, header):
        if header.startswith("HTTP"):
            self.parserstatus(header)
        else:
           self.parseheader(header) 

    def contentfunc(self, chunk):
        if self.cachefile:
            self.cachefile.write(chunk)
            self.writecallback()
        else:
            self.response.cachefile.write(chunk)

    def setopt(self, url=None, verbose=False, nobody=False, headers=None, timeout=120, data=None):
        assert url, "url not passed"
        self.curl.setopt(pycurl.URL, url)

        if verbose:
                self.curl.setopt(pycurl.VERBOSE, 1)
        if nobody:
                self.curl.setopt(pycurl.NOBODY, 1)

        if headers:
            curl_headers = self._header2curlstyle(headers)
            self.curl.setopt(pycurl.HTTPHEADER, curl_headers)

        if data:
            if isinstance(data, str):
                #direct post str as body 
                pass
            elif isinstance(data, dict):
                postfields = self._dict2urlfields(data)
                self.curl.setopt(pycurl.POSTFIELDS, postfields)

        self.curl.setopt(pycurl.TIMEOUT, timeout)
        if self.cookiefile:
            self.curl.setopt(pycurl.COOKIEJAR, self.cookiefile)
            self.curl.setopt(pycurl.COOKIEFILE, self.cookiefile)
        self.curl.setopt(pycurl.HEADERFUNCTION, self.headerfunc)
        self.curl.setopt(pycurl.WRITEFUNCTION, self.contentfunc)

    def post(self, url, **kwargs):
        data = kwargs["data"]
        assert data, "post must have data field"
        assert url, "url must passed!"
        kwargs.update(dict(url=url))
        self.setopt(**kwargs)
        self.curl.perform()
        return self.response

    def close(self):
        self.curl.close()

    def get(self, url, **kwargs):
        assert url, "url must passed!"
        kwargs.update(dict(url=url))
        self.setopt(**kwargs)
        self.curl.perform()
        return self.response


class HTTPSession(object):

    def __init__(self, cookiefile="cookies.txt", **kwargs):
        self.curl = pycurl.Curl()
        self.cookiefile = cookiefile
        self.kwargs = kwargs
        self.response = None
        self.url = ''

    def _resetsession(self):
        self.curl.reset()
    
    def get(self, url, callback=None, cachefile=None, **kwargs):
        self._resetsession()
        if callback and cachefile:
            return httprequest(curl=self.curl, cookiefile=self.cookiefile, callback=callback, cachefile=cachefile).get(url, **kwargs)
        else:
            return httprequest(curl=self.curl, cookiefile=self.cookiefile).get(url, **kwargs)


    def post(self, url, **kwargs):
        self._resetsession()
        return httprequest(curl=self.curl, cookiefile=self.cookiefile).post(url, **kwargs)

    def prepare(self, **kwargs):
        self.response = response(self.curl, **kwargs)
        return self.response

    def start_get(self, url, **kwargs):
        self._resetsession()
        return self.response.get(url, **kwargs)

    def close(self):
        self.curl.close()
        del self.response

def httpget(url, **kwargs):
    return httprequest().get(url, **kwargs)

def httppost(url, **kwargs):
    return httprequest().post(url, **kwargs)


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
        print("save to %s" % (self.cachedir + self.key))    

    def destroy(self):
        try:
            os.unlink(self.cachedir + self.key)
            print("delet cachefile %s" % (self.cachedir + self.key))
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

class SafeLRUCache(LRUCache):
    
    _instance_lock = RLock()

    def __init__(self):
        super(SafeLRUCache, self).__init__()
        self.lock = RLock()

    
    @staticmethod
    def instance():
        if not hasattr(SafeLRUCache, "_instance"):
            with SafeLRUCache._instance_lock:
                if not hasattr(SafeLRUCache, "_instance"):
                    SafeLRUCache._instance = SafeLRUCache()
        return SafeLRUCache._instance

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
    def notify():
        print("got write event")

    buff = CopyOnWriteBuffer()
    session = HTTPSession()
    session.get("http://www.v2ex.com", callback=notify, cachefile=buff)
    print buff.read()
