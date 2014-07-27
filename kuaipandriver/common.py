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
from pycurl import Curl
from threading import RLock
from copy import copy

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

def gethomedir():
    return expanduser("~")

class CopyOnWriteBuffer(object):

    '''实现一个CopyOnWrite 线程安全的缓冲区，读的缓冲区可以一直读，写的'''
    def __init__(self):
        self.buff = ''
        self.buflist = []
        self.lock = RLock()
        self.readindex = 0
        self.length = 0

    def read(self, n=-1):
        if n == -1:
            return self.buff[self.readindex:]
        else:
            return self.buff[self.readindex:self.readindex+n]

    def write(self, chunk):
        with self.lock:
           self.buflist.append(chunk)
           self.length += len(chunk)
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


    def tell(self):
        return self.readindex

    def __len__(self):
        return self.length

    def __del__(self):
        if self.buff:
            del self.buff

class Response(object):

    def __init__(self, curl, url, cookiefile="cookies.txt", cachefile=None, callback=None):
        self.headers = {}
        self.cookiefile = cookiefile
        self.url = url
        self.curl = curl
        self.status_code = -1
        self._cachefile = cachefile
        self.writecallback = callback

    def parseheader(self, header):
        header = header.strip()
        firstsep = header.find(":")
        if firstsep>-1:
            headername = header[0:firstsep]
            headervalue = header[firstsep+2:]
            self.headers[headername] = headervalue

    def __str__(self):
        return "[Response %d]" % self.status_code

    @property
    def cachefile(self):
        return self._cachefile

    @property
    def raw(self):
        return self._cachefile

    @property
    def text(self):
        return self._cachefile.read()

    def json(self):
        import json
        return json.loads(self._cachefile.read()) 

    def parserstatus(self, statusline):
        s1 = statusline.find(" ")
        schema, self.status_code = statusline[0:s1], int(statusline[s1+1:s1+4])

    def headerfunc(self, header):
        if header.startswith("HTTP"):
            self.parserstatus(header)
        else:
           self.parseheader(header) 

    def contentfunc(self, chunk):
        self._cachefile.write(chunk)
        if self.writecallback:
            self.writecallback()

    def _header2curlstyle(self, headers):
        return  map(lambda h:(h[0] +": " + h[1]), headers.iteritems())
    
    def _dict2urlfields(self, payfields):
        return "&".join(["%s=%s" % (item[0],item[1]) for item in payfields.iteritems()])

    def post(self, data=None, verbose=False, headers=None):
        assert data != None, "data parameter not pass"
        if isinstance(data, str):
            #direct post str as body 
            pass
        elif isinstance(data, dict):
            postfields = self._dict2urlfields(data)
            self.curl.setopt(self.curl.POSTFIELDS, postfields)
        self.curl.setopt(self.curl.VERBOSE, verbose)

        if headers:
            heade = self._header2curlstyle(headers)
            print(heade)
            self.curl.setopt(self.curl.HTTPHEADER, heade)

        self.curl.setopt(self.curl.WRITEFUNCTION, self.contentfunc)
        self.curl.setopt(self.curl.HEADERFUNCTION, self.headerfunc)
        self.curl.setopt(self.curl.COOKIEFILE, self.cookiefile)
        self.curl.setopt(self.curl.COOKIEJAR, self.cookiefile)
        self.curl.setopt(self.curl.URL, self.url)
        self.curl.perform()
        return self

    def get(self, verbose=False, nobody=False, headers=None):
        if verbose:
            self.curl.setopt(self.curl.VERBOSE, 1)
        if nobody:
            self.curl.setopt(self.curl.NOBODY, 1)

        if headers:
            curl_headers = self._header2curlstyle(headers)
            self.curl.setopt(self.curl.HTTPHEADER, curl_headers)
        self.curl.setopt(self.curl.COOKIEJAR, self.cookiefile)
        self.curl.setopt(self.curl.COOKIEFILE, self.cookiefile)
        self.curl.setopt(self.curl.HEADERFUNCTION, self.headerfunc)
        self.curl.setopt(self.curl.WRITEFUNCTION, self.contentfunc)

        # if process:
            # self.curl.setopt(self.curl.NOPROGRESS, 0)
            # self.curl.setopt(self.curl.PROGRESSFUNCTION, process)

        self.curl.setopt(self.curl.URL, self.url)
        self.curl.perform()
        return self

class HTTPSession(object):

    def __init__(self, cookiefile="cookies.txt", **kwargs):
        self.curl = Curl()
        self.cookiefile = cookiefile
        self.kwargs = kwargs
        self.response = None
        self.url = ''
        self.cachefile = CopyOnWriteBuffer()

    def _resetsession(self):
        self.curl.reset()
        self.cachefile.clear()
    
    def _setcookiefile(self, url):
        if self.cookiefile:
           self.response =  Response(self.curl, url, cachefile=self.cachefile, cookiefile=self.cookiefile)
        else:
           self.response = Response(self.curl, url, cachefile=self.cachefile)

    def get(self, url, **kwargs):
        self._resetsession()

        if self.cookiefile:
           self.response =  Response(self.curl, url, cachefile=self.cachefile, cookiefile=self.cookiefile)
        else:
           self.response = Response(self.curl, url, cachefile=self.cachefile)

        return self.response.get(**kwargs)

    def post(self, url, **kwargs):
        self._resetsession()
        self._setcookiefile(url)
        return self.response.post(**kwargs)

    def prepare(self, url, **kwargs):
        self.response = Response(self.curl, url, cachefile=self.cachefile, **kwargs)
        self.url = url
        return self.response

    def start_get(self, **kwargs):
        self._resetsession()
        return self.response.get(**kwargs)

    def close(self):
        self.curl.close()
        del self.response


if __name__ == '__main__':
    session = HTTPSession()
    response = session.post("http://www.163.com", verbose=True, data={"name":"alex","sex":"male"})
    if response.status_code == 200:
        cachedfile = response.cachefile
        print cachedfile.read()
