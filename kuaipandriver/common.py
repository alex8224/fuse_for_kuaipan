# -*-coding:utf-8 -*-
#****************************************************
# Author: tony - birdaccp@gmail.com alex8224@gmail.com
# Create by: 2013-08-17 11:01
# Last modified: 2014-07-08 11:45
# Filename: common.py
# Description:
# tolist
# post data, upload file, async client? requests 兼容?
#****************************************************

import os
import re
import hmac
import time
import pycurl
import base64
import hashlib
import traceback
from copy import copy
from threading import RLock
from os.path import expanduser
from Cookie import SimpleCookie
from ConfigParser import ConfigParser
from pkg_resources import resource_filename
from urllib import quote as uquote, quote_plus, urlencode


REG_PROXY = re.compile("^(socks5|socks4|http|https)://(.*):(\d+)$")
REG_COOKIE = re.compile("(.*)=([^;]*)")

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


class response(object):

    def __init__(self):
        self.headers = {}
        self._status_code = -1
        self._cachefile = CopyOnWriteBuffer()
        self._headers = {}
        self.cookieobj = SimpleCookie()
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

class HttpException(Exception):pass

class httprequest(object):
    def __init__(self, curl=None, callback=None, cachefile=None):
        self.curl = curl if curl else pycurl.Curl()
        self.response = response()
        self.writecallback = callback
        self.cachefile = cachefile
        self.cookiedict = {}
        self.ua = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36"

    def _header2curlstyle(self, headers):
        return map(lambda h:(h[0] +": " + h[1]), headers.iteritems())
    
    def _dict2urlfields(self, payfields):
        return urlencode(payfields)

    def parserstatus(self, statusline):
        s1 = statusline.find(" ")
        schema, self.response.status_code = statusline[0:s1], int(statusline[s1+1:s1+4])

    def parsecookies(self, cookie):
        self.response.cookieobj.load(cookie)

    def setcookies(self, cookies):
        cookiedict = {}

        if cookies:
            cookiedict.update(cookies)

        self.cookiedict = cookiedict    

        cookielist = "".join(map(lambda key: "%s=%s;" % (quote_plus(key), quote_plus(cookiedict[key])), cookiedict.iterkeys()))

        if self.cookiedict:
            self.curl.setopt(pycurl.COOKIE, cookielist)
        else:
            self.curl.setopt(pycurl.COOKIELIST, "")

    def parseheader(self, header):
        if not header:
            return
        header = header.strip()
        firstsep = header.find(":")
        if firstsep>-1:
            headername = header[0:firstsep].lower()
            headervalue = header[firstsep+2:]
            self.response.headers[headername] = headervalue
            if headername.lower() == "set-cookie":
                self.parsecookies(headervalue)

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

    def setproxy(self, proxy):
        if not proxy:
            return
        matchgroups = REG_PROXY.match(proxy)
        if matchgroups:
            proto, host, port = matchgroups.groups()
            if proto not in ("socks5", "http"):
                return
            if proto == "socks5":
                self.curl.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS5_HOSTNAME)

            self.curl.setopt(pycurl.PROXY, host + ":" + port)

    def setopt(self, method='GET', ua='', cookies=None, proxy=None, url=None, verbose=False, headers=None, timeout=120, data=None, allow_redirect=False):
        '''
        @proxy protocol://host:port eg: socks5://127.0.0.1:1080
        '''
        self.curl.reset()
        method = method.upper() 

        if method not in ("GET", "POST", "DELETE", "PUT", "OPTIONS", "HEAD"):
            raise pycurl.error("not support method:%s" % method)

        if method in ("HEAD", "DELETE"):
            self.curl.setopt(pycurl.NOBODY, False)

        if method in ("POST", "PUT"):
            self.curl.setopt(pycurl.POST, True)

        if method in ("PUT", "DELETE", "PUT", "OPTIONS"):
            self.curl.setopt(pycurl.CUSTOMREQUEST, method)

        self.curl.setopt(pycurl.NOSIGNAL, True)
        self.curl.setopt(pycurl.URL, url)

        self.setproxy(proxy)
        
        if verbose:
                self.curl.setopt(pycurl.VERBOSE, True)

        allheaders = []

        self.setcookies(cookies)

        if headers:
            curl_headers = self._header2curlstyle(headers)
            allheaders.extend(curl_headers)

        if ua:
            self.ua = ua

        allheaders.extend(["User-Agent: %s" % self.ua])

        if allheaders:
            self.curl.setopt(pycurl.HTTPHEADER, allheaders)

        if method in ("POST", "PUT"):
            if isinstance(data, str):
                self.curl.setopt(pycurl.POSTFIELDS, data)
            elif hasattr(data, "read"):
                self.curl.setopt(pycurl.UPLOAD, True)
                self.curl.setopt(pycurl.READFUNCTION, data.read)
                data.seek(0, 2)
                filesize = data.tell()
                data.seek(0)
                self.curl.setopt(pycurl.INFILESIZE, filesize)
            elif isinstance(data, dict):
                postfields = self._dict2urlfields(data)
                self.curl.setopt(pycurl.POSTFIELDS, postfields)

        self.curl.setopt(pycurl.TIMEOUT, timeout)
        # if self.cookiefile:
            # self.curl.setopt(pycurl.COOKIEJAR, self.cookiefile)
            # self.curl.setopt(pycurl.COOKIEFILE, self.cookiefile)
        self.curl.setopt(pycurl.HEADERFUNCTION, self.headerfunc)
        self.curl.setopt(pycurl.WRITEFUNCTION, self.contentfunc)

        if allow_redirect:
            self.curl.setopt(pycurl.FOLLOWLOCATION, 1)
            self.curl.setopt(pycurl.MAXREDIRS, 5) 
            
    def _method(self, methodname, url, **kwargs):
        try:
            kwargs.update(dict(url=url, method=methodname))
            self.setopt(**kwargs)
            self.curl.perform()
            return self.response
        except pycurl.error, error:
            raise HttpException(error)

    def post(self, url, **kwargs):
        assert "data" in kwargs, "post must have data field"
        return self._method("POST", url, **kwargs)

    def get(self, url, **kwargs):
        return self._method("GET", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._method("DELETE", url, **kwargs)

    def put(self, url, **kwargs):
        assert "data" in kwargs, "put must have data field"
        return self._method("PUT", url, **kwargs)

    def head(self, url, **kwargs):
        return self._method("HEAD", url, **kwargs)

    def getandclose(self, url, **kwargs):
        try:
            return self.get(url, **kwargs)
        except:
            raise
        finally:
            self.curl.close()

    def postandclose(self, url, **kwargs):
        try:
            return self.post(url, **kwargs)
        except:
            raise
        finally:
            self.curl.close()

class HTTPSession(object):

    def __init__(self):
        self.curl = pycurl.Curl()

    def get(self, url, callback=None, cachefile=None, **kwargs):
        return httprequest(curl=self.curl, callback=callback, cachefile=cachefile).get(url, **kwargs)

    def post(self, url, **kwargs):
        return httprequest(curl=self.curl).post(url, **kwargs)

    def put(self, url, **kwargs):
        return httprequest(curl=self.curl).put(url, **kwargs)
    
    def delete(self, url, **kwargs):
        return httprequest(curl=self.curl).delete(url, **kwargs)

    def head(self, url, **kwargs):
        return httprequest(curl=self.curl).head(url, **kwargs)

    def close(self):
        if self.curl:
            self.curl.close()

def httpget(url, **kwargs):
    return httprequest().getandclose(url, **kwargs)

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