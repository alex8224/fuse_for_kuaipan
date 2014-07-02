#!/usr/bin/python
# -*-coding:utf-8 -*-
#****************************************************
#Author: tony - birdaccp@gmail.com alex8224@gmail.com
#Create by: 2013-08-17 10:33
#Last modified:2014-07-02 15:01:35
#Filename: kuaipan.py
#Description:
#****************************************************


import os
import sys
import urllib
import signal
import common
import requests
import traceback
import lxml.html as html
from config import Config

config = Config()

def handler(signum, frame):
    print "use press ctrl+c exit"
    sys.exit(0)
signal.signal(signal.SIGINT, handler)


class OpenAPIError(Exception):
    pass

class OpenAPIHTTPError(OpenAPIError):
    def __init__(self, status, msg):
        self.status = status
        self.msg = msg

    def __str__(self):
        return "%s - %s" % (self.status, self.msg)

class KuaipanAPI(object):
    VERSION = "1.0"
    SIG_METHOD = "HMAC-SHA1"

    def __init__(self):
        self.consumer_key = config.get_consumer_key()
        self.consumer_secret = config.get_consumer_secret()
        self.auth_user = config.get_auth_user()
        self.auth_pwd = config.get_auth_pwd()
        self.login()

    def login(self):
        self.request_token()
        authorize_url = self.authorize()
        auth_code = self.get_auth_code(authorize_url)
        self.access_token(auth_code)

    def request_token(self, callback=None):
        sig_req_url = self.__get_sig_url("request_token_base_url",has_oauth_token=False)
        rf = requests.get(sig_req_url)
        status = rf.status_code
        if status == 200:
            d = rf.json()
            for k in (u'oauth_token', u'oauth_token_secret', u'oauth_callback_confirmed'):
                if d.has_key(k):
                    v = d.get(k)
                    setattr(self, common.to_string(k), common.safe_value(v))
        else:
            raise OpenAPIHTTPError(status, rf.read())

    def authorize(self):
        return config.get_authorize_url() % self.oauth_token

    def get_auth_code(self, url):

        ua = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36"
        sess = requests.Session()
        referer = url
        step1_request = sess.get(url)
        htdoc = html.fromstring(step1_request.text)
        s, app_name, oauth_token = htdoc.xpath("//input[@type='hidden']")
        post_payload = {"username":self.auth_user, "userpwd":self.auth_pwd,"s":s.value, "app_name":app_name.value, "oauth_token":oauth_token.value}
        headers = {"User-Agent":ua,"Referer":referer, "Host":"www.kuaipan.cn"}
        posturl = "https://www.kuaipan.cn/api.php?ac=open&op=authorisecheck"

        step2_request = sess.post(posturl, data=post_payload,headers=headers)
        htdoc = html.fromstring(step2_request.text)
        return htdoc.xpath("//strong")[0].text

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
                        "upload":config.get_fileops_upload_suffix()
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
        parameters = self._oauth_parameter()
        parameters["oauth_verifier"] = auth_code
        base_url = config.get_access_token_base_url()
        sig_req_url = self._sig_request_url(base_url, parameters)
        req_accesstoken = requests.get(sig_req_url)
        tokeninfo = req_accesstoken.json()
        self.oauth_token = str(tokeninfo["oauth_token"])
        self.oauth_token_secret = str(tokeninfo["oauth_token_secret"])
        self.charged_dir = str(tokeninfo["charged_dir"])
        self.userid = tokeninfo["user_id"]

    def account_info(self):
        sig_req_url = self.__get_sig_url("account_info")
        req_accinfo = requests.get(sig_req_url)
        return req_accinfo.json()

    def metadata(self,root="app_folder", path=''):
        sig_req_url = self.__get_sig_url("metadata", urlsuffix=(root, urllib.quote(path.encode("utf-8"))))
        metadata_result = requests.get(sig_req_url)
        return metadata_result.json() if metadata_result.status_code==200 else metadata_result


    def download_file(self, filepath):
        attach = {"path":filepath, "root":"app_folder"}
        sig_req_url = self.__get_sig_url("download", attachdata=attach)

        try:
            req_download = requests.get(sig_req_url,stream=True)
            fd = req_download.raw
            count = 0
            bufsize = 8192
            # pdf = open("/tmp/abc", "w")
            while 1:
                buff = fd.read(bufsize)
                if not buff:
                    break
                # pdf.write(buff)
                bufsize = (yield buff)
                count += len(buff)
        except Exception, e:
            traceback.print_exc()


    def create_folder(self, folder,dir=""):
        attach = {"root":"app_folder", "path":folder}
        sig_req_url = self.__get_sig_url("create_folder", attachdata=attach)
        return requests.get(sig_req_url)

    def upload(self, uploadpath, name):

        def get_upload_locate():
            locate_url = self.__get_sig_url("upload_locate")
            resp = requests.get(locate_url)
            return resp, resp.json()

        # import pdb;pdb.set_trace()
        result, upload_url = get_upload_locate()
        basename = os.path.basename(name)
        upload_filename = uploadpath + "/" + basename
        attach = {"overwrite":"True","root":"app_folder","path":upload_filename}
        upload_url = self.__get_sig_url("upload",urlsuffix=(upload_url["url"],), attachdata=attach, httpmethod="post")
        files = {'file': (urllib.quote(basename.encode("utf-8")), open(name, 'rb'))}
        return requests.post(upload_url, files=files)

    def delete(self, filename):
        attach = {"root":"app_folder","path":filename}
        sig_req_url = self.__get_sig_url("delete", attachdata=attach)
        delete_result = requests.get(sig_req_url)
        return delete_result

    def copy(self, frompath, topath):
        attach = {"root":"app_folder", "from_path":frompath, "to_path":topath}
        sig_req_url = self.__get_sig_url("copy", attachdata=attach)
        copy_result = requests.get(sig_req_url)
        return copy_result.json() if copy_result.status_code == 200 else copy_result

    def move(self, frompath, topath):
        attach = {"root":"app_folder","from_path":frompath, "to_path":topath}
        sig_req_url = self.__get_sig_url("move", attachdata=attach)
        move_result = requests.get(sig_req_url)
        # return move_result.json() if move_result.status_code == 200 else move_result
        return move_result

    def _oauth_parameter(self, has_token=True):
        parameters = {
                'oauth_consumer_key': self.consumer_key,
                'oauth_timestamp': common.timestamp(),
                'oauth_nonce': common.random_string(),
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
