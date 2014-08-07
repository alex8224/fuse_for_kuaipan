# -*-coding:utf-8 -*-
#********************************************************************************
#Author: alex8224@gmail.com
#Create by: 2013-08-17 12:53
#Last modified: 2014-07-01
#Filename: httputils.py
#Description: 
# 是一个用于发送 http请求，类似与 urllib 的工具,  基于 pycurl 实现
# 部分实现了 requests 的接口, 如 requests.get/request.post/request.put/request.delete/requests.head
# 不支持 requests.get(url, stream=True) 这样的写法
#********************************************************************************

import re
import pycurl
from common import CopyOnWriteBuffer
from Cookie import SimpleCookie
from urllib import urlencode, quote_plus

REG_PROXY = re.compile("^(socks5|socks4|http|https)://(.*):(\d+)$")
REG_COOKIE = re.compile("(.*)=([^;]*)")

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

    def __repr__(self):
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
        self._curl = curl if curl else pycurl.Curl()
        self.response = response()
        self.writecallback = callback
        self.cachefile = cachefile
        self.cookiedict = {}
        self.ua = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36"

    @property
    def curlhandler(self):
        return self._curl

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
            self._curl.setopt(pycurl.COOKIE, cookielist)
        else:
            self._curl.setopt(pycurl.COOKIELIST, "")

    def parseheader(self, header):
        if not header:
            return
        header = header.strip()
        firstsep = header.find(":")
        if firstsep > -1:
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
                self._curl.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS5_HOSTNAME)

            self._curl.setopt(pycurl.PROXY, host + ":" + port)

    def setopt(self, method='GET', ua='', cookies=None, proxy=None, url=None, verbose=False, headers=None, timeout=120, data=None, allow_redirect=False):
        '''
        @proxy protocol://host:port eg: socks5://127.0.0.1:1080
        '''
        self._curl.reset()
        method = method.upper() 

        if method not in ("GET", "POST", "DELETE", "PUT", "OPTIONS", "HEAD"):
            raise pycurl.error("not support method:%s" % method)

        if method in ("HEAD", "DELETE"):
            self._curl.setopt(pycurl.NOBODY, False)

        if method in ("POST", "PUT"):
            self._curl.setopt(pycurl.POST, True)

        if method in ("PUT", "DELETE", "PUT", "OPTIONS"):
            self._curl.setopt(pycurl.CUSTOMREQUEST, method)

        self._curl.setopt(pycurl.NOSIGNAL, True)
        self._curl.setopt(pycurl.URL, url)

        self.setproxy(proxy)
        
        if verbose:
                self._curl.setopt(pycurl.VERBOSE, True)

        allheaders = []

        self.setcookies(cookies)

        if headers:
            curl_headers = self._header2curlstyle(headers)
            allheaders.extend(curl_headers)

        if ua:
            self.ua = ua

        allheaders.extend(["User-Agent: %s" % self.ua])

        if allheaders:
            self._curl.setopt(pycurl.HTTPHEADER, allheaders)

        if method in ("POST", "PUT"):
            if isinstance(data, str):
                self._curl.setopt(pycurl.POSTFIELDS, data)
            elif hasattr(data, "read"):
                self._curl.setopt(pycurl.UPLOAD, True)
                self._curl.setopt(pycurl.READFUNCTION, data.read)
                data.seek(0, 2)
                filesize = data.tell()
                data.seek(0)
                self._curl.setopt(pycurl.INFILESIZE, filesize)
            elif isinstance(data, dict):
                postfields = self._dict2urlfields(data)
                self._curl.setopt(pycurl.POSTFIELDS, postfields)

        self._curl.setopt(pycurl.TIMEOUT, timeout)
        # if self.cookiefile:
            # self._curl.setopt(pycurl.COOKIEJAR, self.cookiefile)
            # self._curl.setopt(pycurl.COOKIEFILE, self.cookiefile)
        self._curl.setopt(pycurl.HEADERFUNCTION, self.headerfunc)
        self._curl.setopt(pycurl.WRITEFUNCTION, self.contentfunc)

        if allow_redirect:
            self._curl.setopt(pycurl.FOLLOWLOCATION, 1)
            self._curl.setopt(pycurl.MAXREDIRS, 5) 
            
    def _method(self, methodname, url, **kwargs):
        try:
            kwargs.update(dict(url=url, method=methodname))
            self.setopt(**kwargs)
            self._curl.perform()
            return self.response
        except pycurl.error, error:
            raise HttpException(error)

    def lazymethod(self, methodname, url, **kwargs):
        kwargs.update(dict(url=url, method=methodname))
        self.setopt(**kwargs)
        return self

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
            self._curl.close()

    def postandclose(self, url, **kwargs):
        try:
            return self.post(url, **kwargs)
        except:
            raise
        finally:
            self._curl.close()

class BatchRequest(object):

    def __init__(self):
        self._handler_pool= pycurl.CurlMulti()
        self._response = []

    def _method(self, methodname, url, **kwargs):
        req = httprequest()
        handle = req.lazymethod(methodname, url, **kwargs).curlhandler
        self._handler_pool.add_handle(handle)
        self._response.append(req.response)
        return handle

    def get(self, url , **kwargs):
        return self._method("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._method("POST", url, **kwargs)

    def head(self, url, **kwargs):
        return self._method("HEAD", url, **kwargs)

    def put(self, url, **kwargs):
        return self._method("PUT", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._method("DELET", url, **kwargs)

    def run(self):
        while 1:
            ret, num_handles = self._handler_pool.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM: break
        while num_handles:
            ret = self._handler_pool.select(1.0)
            if ret == -1: continue
            while 1:
                ret, num_handles = self._handler_pool.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM: break
  
    def info_read(self):
        return self._handler_pool.info_read()

    def runinbackground(self):
        pass

    def get_result(self):
        return self._response

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

if __name__ == '__main__':
    print httpget("http://www.163.com").headers
