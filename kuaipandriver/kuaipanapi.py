#!/usr/bin/python
# -*-coding:utf-8 -*-
#****************************************************
#Author: tony - birdaccp@gmail.com alex8224@gmail.com
#Create by: 2013-08-17 10:33
#Last modified:2014-07-02 15:01:35
#Filename: kuaipanapi.py
#Description:
#****************************************************


import sys
import urllib
import signal
import common
import requests
from functools import wraps
import lxml.html as html
from common import oauth_once_next, HTTPSession, httpget, config, Singleton, Context
from requests.exceptions import RequestException

next_oauth_once = oauth_once_next()

context = Context.instance()

def handler(signum, frame):
    print "use press ctrl+c exit"
    sys.exit(0)
signal.signal(signal.SIGINT, handler)


class OpenAPIError(Exception):
    def __init__(self, exception, traceobj):
        super(OpenAPIError, self).__init__(exception, None, traceobj)


class OpenAPIException(Exception):pass


def catchexception(func):

    @wraps(func)
    def catch(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            _, exception, tracebackinfo  = sys.exc_info()
            raise OpenAPIError(exception, tracebackinfo)
    return catch    


class KuaipanAPI(Singleton):

    VERSION = "1.0"
    SIG_METHOD = "HMAC-SHA1"

    def __init__(self):
        self.mntpoint = context.mntpoint
        self.consumer_key = ''
        self.consumer_secret = ''
        self.auth_user = context.user
        self.auth_pwd = context.pwd
        self.applist = context.keylist
        self.keylist = self.getapikey()
        self.login()

    def getapikey(self):
        for key, secret in self.applist:
            yield str(key), str(secret) 

    def login(self):
        try:
           self.consumer_key, self.consumer_secret =  self.keylist.next()
        except StopIteration:
            raise OpenAPIException("no more api secret and key can be used!")

        self.request_token()
        authorize_url = self.authorize()
        auth_code = self.get_auth_code(authorize_url)
        self.access_token(auth_code)
        from kuaipandriver.common import savelogin
        savelogin(self.mntpoint, self.consumer_key, self.consumer_secret, self.auth_user, self.auth_pwd)

    def retrylogin(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            this = args[0]
            try:
                while 1:
                    result = func(*args, **kwargs)
                    if result.status_code == 401 and result.json()["msg"] == "api daily limit":
                        this.login()
                    else:
                        return result
            except:
                raise
        return wrapper                

    @retrylogin
    def request_token(self, callback=None):
        sig_req_url = self.__get_sig_url("request_token_base_url", has_oauth_token=False)
        try:
            result = httpget(sig_req_url)
            msg = ''
            if result.status_code == 200:
                msg = result.json()
                for k in (u'oauth_token', u'oauth_token_secret', u'oauth_callback_confirmed'):
                    if msg.has_key(k):
                        v = msg.get(k)
                        setattr(self, common.to_string(k), common.safe_value(v))
            elif result.status_code == 500:
                raise RequestException("kuaipan internal server error!")
            return result
        except RequestException as reqex:
            errmsg = "get init token failed!, err message is:%s" % str(reqex)
            print(errmsg)
            sys.exit(1)
            

    def authorize(self):
        return config.get_authorize_url() % self.oauth_token

    def get_auth_code(self, url):
        try:
            sess = HTTPSession()
            referer = url
            step1_request = sess.get(url)
            htdoc = html.fromstring(step1_request.text)
            s, app_name, oauth_token = htdoc.xpath("//input[@type='hidden']")
            post_payload = {"username":self.auth_user, "userpwd":self.auth_pwd,"s":s.value, "app_name":app_name.value, "oauth_token":oauth_token.value}
            headers = {"Referer":referer, "Host":"www.kuaipan.cn"}
            posturl = "https://www.kuaipan.cn/api.php?ac=open&op=authorisecheck"

            step2_request = sess.post(posturl, data=post_payload,headers=headers)
            htdoc = html.fromstring(step2_request.text)
            msg = htdoc.xpath("//strong")[0].text
            if not msg.isdigit():
                raise RequestException(msg.encode("utf-8"))
            return msg
        except RequestException as reqex:
            errmsg = "get auth code failed!, error message is:%s" % str(reqex)
            print(errmsg)
            raise

    def __get_sig_url(self, method, urlsuffix=None, attachdata=None, httpmethod="get",has_oauth_token=True):
        method_url = {
                        "request_token_base_url":config.get_request_token_base_url(),
                        "access_token_base_url":config.get_access_token_base_url(),
                        "account_info":config.get_account_info_base_url(),
                        "metadata":config.get_metadata_base_url(),
                        "create_folder":config.get_fileops_create_base_url(),
                        "download":config.get_fileops_download_base_url(),
                        "delete":config.get_fileops_delete_base_url(),
                        "move":config.get_fileops_move_base_url(),
                        "copy":config.get_fileops_copy_base_url(),
                        "upload_locate":config.get_fileops_upload_locate_base_url(),
                        "upload":config.get_fileops_upload_suffix(),
                        "convert":config.get_fileops_convert_url()
                     }

        parameters = self._oauth_parameter(has_token=has_oauth_token)
        if attachdata:
            for k,v in attachdata.iteritems():
                parameters[k] = v

        base_url = method_url.get(method, None)

        if base_url:
            if urlsuffix:
                base_url = str(base_url % urlsuffix)
            return self._sig_request_url(base_url, parameters,method=httpmethod)

    def access_token(self, auth_code):
        try:
            parameters = self._oauth_parameter()
            parameters["oauth_verifier"] = auth_code
            base_url = config.get_access_token_base_url()
            sig_req_url = self._sig_request_url(base_url, parameters)
            req_accesstoken = httpget(sig_req_url)

            if req_accesstoken.status_code != 200:
                raise RequestException(req_accesstoken.json()["msg"])

            tokeninfo = req_accesstoken.json()
            self.oauth_token = str(tokeninfo["oauth_token"])
            self.oauth_token_secret = str(tokeninfo["oauth_token_secret"])
            self.charged_dir = str(tokeninfo["charged_dir"])
            self.userid = tokeninfo["user_id"]
        except RequestException as reqex:
            errmsg = "get access token failed!, err message is:%s" % str(reqex)
            print(errmsg)
            sys.exit(1)


    @catchexception
    @retrylogin
    def account_info(self):
        sig_req_url = self.__get_sig_url("account_info")
        return httpget(sig_req_url)

    @catchexception
    @retrylogin
    def metadata(self,root="app_folder", path="", session=None):
        sig_req_url = self.__get_sig_url("metadata", urlsuffix=(root, urllib.quote(path.encode("utf-8"))))
        if session:
            return session.get(sig_req_url)
        else:
            return httpget(sig_req_url)

    @catchexception
    @retrylogin
    def get_downloadurl(self, filepath, session):
        attach = {"path":filepath, "root":"app_folder"}
        sig_req_url = self.__get_sig_url("download", attachdata=attach)
        return session.get(sig_req_url)

    @catchexception
    @retrylogin
    def download_file2(self, url, callback, cachefile, session):
        return session.get(url, callback=callback, cachefile=cachefile)

    @catchexception
    @retrylogin
    def create_folder(self, folder,dir=""):
        attach = {"root":"app_folder", "path":folder}
        sig_req_url = self.__get_sig_url("create_folder", attachdata=attach)
        return httpget(sig_req_url)

    @catchexception
    @retrylogin
    def get_upload_locate(self, ):
        locate_url = self.__get_sig_url("upload_locate")
        return httpget(locate_url)

    @catchexception
    def upload(self, uploadpath, fullpath, filename):
        result = self.get_upload_locate()
        if result.status_code != 200:
            raise OpenAPIException(result.text)

        upload_url = result.json()
        upload_filename = uploadpath + "/" + filename
        attach = {"overwrite":"True","root":"app_folder","path":upload_filename}
        upload_url = self.__get_sig_url("upload",urlsuffix=(upload_url["url"],), attachdata=attach, httpmethod="post")
 
        try:
            return self.curl_upload(upload_url, fullpath, filename)
        except:
            return self.default_upload(upload_url, fullpath, filename)


    def curl_upload(self, upload_url, fullpath, filename):
        import subprocess
        from subprocess import PIPE
        cmd = '''curl -D /dev/stdout -F "filedata=@%s" "%s"''' % (fullpath, upload_url)
        p = subprocess.Popen(cmd, stdout=PIPE,stderr=PIPE, shell=True)
        upload_result = p.wait()
        print("using curl to upload file %s with filename %s" % (fullpath, filename))
        errormsg = p.stdout.read() + "\r\n" + p.stderr.read()
        print(errormsg)
        if upload_result == 0:
            return True
        else:
            return False

    def default_upload(self, upload_url, fullpath, filename):
        try:
            files = {'file': (urllib.quote(filename.encode("utf-8")), open(fullpath, 'rb'))}
            return requests.post(upload_url, files=files).status_code == 200
        except:
            _, exception, tracebackinfo  = sys.exc_info()
            raise OpenAPIError(exception, tracebackinfo)


    @catchexception
    @retrylogin
    def delete(self, filename):
        attach = {"root":"app_folder","path":filename, "to_recycle":False}
        sig_req_url = self.__get_sig_url("delete", attachdata=attach)
        return httpget(sig_req_url)

    @catchexception
    @retrylogin
    def copy(self, frompath, topath):
        attach = {"root":"app_folder", "from_path":frompath, "to_path":topath}
        sig_req_url = self.__get_sig_url("copy", attachdata=attach)
        return httpget(sig_req_url)

    @catchexception
    @retrylogin
    def move(self, frompath, topath):
        attach = {"root":"app_folder","from_path":frompath, "to_path":topath}
        sig_req_url = self.__get_sig_url("move", attachdata=attach)
        return httpget(sig_req_url)

    @catchexception
    @retrylogin
    def convert(self, path, viewtype):
        doctype = path[-3:]
        attach = {"type":doctype, "view":viewtype, "root":"app_folder", "path":path, "zip":1}
        sig_req_url = self.__get_sig_url("convert", attachdata=attach)
        session = HTTPSession()
        convertreq = session.get(sig_req_url)
        if convertreq.status_code == 302:
            converturl = convertreq.headers["location"]
            return session.get(converturl)
        else:
            return convertreq

    def _oauth_parameter(self, has_token=True):
        parameters = {
                'oauth_consumer_key': self.consumer_key,
                'oauth_timestamp': common.timestamp(),
                'oauth_nonce': next_oauth_once(),
                'oauth_signature_method': self.SIG_METHOD,
                'oauth_version': self.VERSION,
                }
        if has_token:
            parameters['oauth_token'] = self.oauth_token
        return parameters

    def _secret_key(self, has_token=True):
        s = self.consumer_secret + '&'
        if has_token:
            s += str(self.oauth_token_secret)
        return s

    def _sig_request_url(self, base, p, method='get'):
        has_token = True if p.has_key('oauth_token') else False
        oauth_signature = common.generate_signature(base, p, self._secret_key(has_token=has_token), method)
        return common.get_request_url(base, p, oauth_signature)


def test_api_limit():
    import sys
    k,s,u,p = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    api = KuaipanAPI("mnt", k,s,u,p)
    import sys
    stdout = sys.stdout
    session = HTTPSession()
    count = 0
    while 1:
        try:
            result = api.metadata(path="/", session=session)
            if result.status_code != 200:
                print result.text
                break
            else:
                count += 1
            stdout.write("api call count: %d\r" % (count + 1))
            stdout.flush()
        except:
            break

    print "api limit is: %d" % count

def test_doc_convert():
    mnt, key, secret, user, pwd = "mnt", "", "", "", ""
    api = KuaipanAPI(mnt, key, secret, user, pwd)
    path, viewtype = sys.argv[1], sys.argv[2]
    result = api.convert(path,viewtype)
    if result.status_code == 200:
        fd = result.raw
        zipfile = open(path +".zip", "w")
        while 1:
            data = fd.read(8182)
            if data:
                zipfile.write(data)
            else:
                break
        print "convert ok"    
    else:
        print "convert failed!"
        print result.text
