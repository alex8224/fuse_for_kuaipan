#!/usr/bin/python
# -*-coding:utf-8 -*-

#********************************************************************************
#Author: tony - birdaccp@gmail.com alex8224@gmail.com
#Create by: 2013-08-17 12:53
#Last modified: 2014-07-01
#Filename: kuanpanfuse.py
#Description: 实现快盘的ＡＰＩ,可以实现文件的异步上传, 下载，浏览，目录操作
# 缩略图和版本功能未实现
#********************************************************************************

import os
import sys
import time
import copy
import signal
import common
import logging
from sys import argv
from shutil import move
from hashlib import sha1
from functools import partial
from kuaipanapi import KuaipanAPI
from Queue import Queue, Empty
from stat import S_IFDIR, S_IFREG
from threading import Thread, Event,  RLock as Lock
from errno import ENOENT,EROFS, ENOTEMPTY, EEXIST, EIO
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


TYPE_DIR= (S_IFDIR | 0644 )
TYPE_FILE= (S_IFREG | 0644 )

CACHE_PATH = "/tmp/"

ROOT_ST_INFO = {
        "st_mtime": common.timestamp(),
        "st_mode":  TYPE_DIR,
        "st_size":  4096,
        "st_gid":   0,
        "st_uid":   0,
        "st_atime": common.timestamp()
        }

def getLogger():
    logger = logging.getLogger("kuaipanserver")
    hdlr = logging.StreamHandler()
    fdlr = logging.handlers.TimedRotatingFileHandler("kuaipan_fuse.log", 'D', backupCount=30)
    fm = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    logger.addHandler(hdlr)
    logger.addHandler(fdlr)
    hdlr.setFormatter(fm)
    fdlr.setFormatter(fm)
    logger.setLevel(logging.DEBUG)
    return logger

logger = getLogger()

def handler(signum, frame):
    print "use press ctrl+c exit"
    sys.exit(0)

signal.signal(signal.SIGINT, handler)

class Future(object):

    def __init__(self, notify = None):
        self.result = None
        self.finished = False
        self.notify = notify

    def get(self):
        while not self.is_finished:
            time.sleep(1)
        self.notify(self.result)

    def is_finished(self):
        return self.finished


class TaskPool(object):
    def __init__(self):
        self.uploadpool = {}
        self.downloadpool = {}
        self.uploadlock, self.downlock = Lock(), Lock()
        self.taskclass = {"upload":WriteTask,"download":DownloadTask}

    def upload_file(self, hashpath, *args):
        return self.__new_task("upload", hashpath, *args)

    def download_file(self, hashpath, *args):
        return self.__new_task("download", hashpath, *args)

    def __task_sucess(self, tasktype, key, result=None):
        lock = self.__getlock(tasktype)
        with lock:
            logger.debug("task %s already complted!, delete it!" % key)
            pool = self.__getpool(tasktype)
            if pool.has_key(key):
                del pool[key]

    def __getlock(self, tasktype):
        return self.uploadlock if tasktype == "upload" else self.downlock

    def __getpool(self, tasktype):
        return self.uploadpool if tasktype == "upload" else self.downloadpool

    def __new_task(self, tasktype, key, *args):
        callback_wrapper = partial(self.__task_sucess, tasktype, key)
        taskparams = list(args)
        taskparams.insert(1, key)
        taskparams.extend([callback_wrapper])
        task = self.taskclass[tasktype](*taskparams)
        with self.__getlock(tasktype):
            pool = self.__getpool(tasktype)
            pool[key] = task
            task.start()
            return task

    def query_upload_task(self, key):
        return self.__query_task("upload", key)

    def query_download_task(self, key):
        return self.__query_task("download", key)

    def __query_task(self, tasktype, key):
        with self.__getlock(tasktype):
            pool = self.__getpool(tasktype)
            if pool.has_key(key):
               return pool[key]

class DownloadTask(Future):

    def __init__(self, api, hashpath, path, notify):
        Future.__init__(self)
        self.api = api
        self.path = path
        self.notify = notify
        self.waiter = Event()
        self.isfirst = True
        self.lock = Lock()

    def wait_data(self,size):
        '''等待达到指定的ｓｉｚｅ后返回给调用端'''
        self.waiter.wait()
        with self.lock:
            if self.isfirst:
                firstdata = self.buffgen.send(None)
                self.isfirst = False
                return firstdata
            else:
                try:
                    data = self.buffgen.send(size)
                    return data
                except StopIteration:
                    self.buffgen.close()

    def end_download_file(self):
            self.notify()

    def start(self):
        '''调用下载文件的ＡＰＩ，在数据可用时通知调用端开始下载数据'''
        logger.debug("start download %s =================" % self.path)
        self.buffgen = self.api.download_file(self.path)
        self.waiter.set()

class WriteTask(Thread, Future):
    '''用来调用ＡＰＩ上传文件'''

    def __init__(self, api, hashpath, path, filename, data, offset, notify):
        Thread.__init__(self)
        Future.__init__(self)
        self.api = api
        self.hashpath = hashpath
        self.path = path
        self.filename = filename
        self.fullpath = CACHE_PATH + hashpath
        self.writebytes = 0
        self.cmd = Queue(1000)
        self.cmd.put(("start_upload_file",(data, offset)))
        self.notify = notify
        self.clsname = "_" + self.__class__.__name__

    def sendmesg(self, cmd):
        self.cmd.put(cmd)


    def start_upload_file(self, data, offset):
        self.sendmesg(("start_upload_file", (data, offset)))

    def __start_upload_file(self, data, offset):
        '''启动缓存文件的写入'''
        logger.debug("====================call start_upload_file======================")
        if os.path.exists(self.fullpath):
            os.unlink(self.fullpath)

        with open(self.fullpath, "w") as f:
            f.seek(offset)
            f.write(data)
            self.writebytes += len(data)
            logger.debug("written %d bytes" % self.writebytes)

    def push_upload_file(self, data, offset):
        self.sendmesg(("push_upload_file", (data, offset)))

    def __push_upload_file(self, data, offset):
        '''将数据添加到对应的缓存文件中'''

        logger.debug("====================call push_upload_file======================")
        if os.path.exists(self.fullpath):
            with open(self.fullpath, "a") as f:
                f.seek(offset)
                f.write(data)
                self.writebytes += len(data)
                logger.debug("written %d bytes" % self.writebytes)

    def end_upload_file(self):
        self.sendmesg(("end_upload_file", ()))

    def __end_upload_file(self):
        '''开始真正的上传文件'''
        try:
            #import pdb;pdb.set_trace()
            logger.debug("start upload file %s to kuaipan server" % self.path)
            uploadpath = os.path.dirname(self.path)
            destpath = CACHE_PATH + self.filename
            move(self.fullpath, destpath)
            self.fullpath = destpath
            uploadresult = self.api.upload(uploadpath, destpath)
            if uploadresult.status_code == 200:
                logger.debug("file %s upload ok" % destpath)
                self.result = destpath
            else:
                logger.error("file %s upload failed!" % destpath)
                self.result = FuseOSError(ENOENT)
        finally:
            self.sendmesg("quit")
            self.is_finished = True
            self.notify(self.result)
            self.__dropcache()

    def __dropcache(self):
        try:
            os.unlink(self.fullpath)
        except Exception,e:
            logger.error(e)

    def __callmethod(self, mesg):
        methodname, args = mesg
        internalmethod = self.clsname + "__" + methodname
        if hasattr(self, internalmethod):
            logger.debug("call method %s " % (methodname, ))
            return getattr(self, internalmethod)(*args)
        else:
            logger.error("no such method %s, params: %s" % (methodname, args))

    def run(self):
        logger.debug("write task for %s started!" % self.path)
        while 1:
            try:
                cmd = self.cmd.get()
                if cmd != "quit":
                    self.__callmethod(cmd)
                else:
                    break
            except Empty:
                pass

class KuaiPanFuse(LoggingMixIn, Operations):

    def __init__(self):
        self.api = KuaipanAPI()
        self.root = "."
        self.fd = 0
        self.fileprops = {"/":ROOT_ST_INFO}
        self.taskpool = TaskPool()
        self.rootfiles = self.listdir("/")

    def listdir(self, path="/testupload2"):
        '''
        [{u'create_time': u'2014-06-26 14:50:00',
             u'file_id': u'7774526460920101',
             u'is_deleted': False,
             u'modify_time': u'2014-06-26 14:50:00',
             u'name': u'Deploying.OpenStack.Jul.2011.pdf',
             u'rev': u'1',
             u'sha1': u'87f2d5506fafffbc2928404ee5fe7ea118cc2ca8',
             u'share_id': u'0',
             u'size': 6199737,
             u'type': u'file'}]


             容易使用的结构
             {"path":st_info}
        '''
        metadata_for_dir = self.api.metadata(path=path)
        for finfo in metadata_for_dir["files"]:
            path_as_key = path + finfo["name"] if path == "/" else path + "/" + finfo["name"]
            st_info = {
                        "st_mtime": common.to_timestamp(finfo["modify_time"]),
                        "st_mode":  TYPE_FILE if finfo["type"] == "file" else TYPE_DIR,
                        "st_size":  int(finfo["size"]),
                        "st_gid":   0,
                        "st_uid":   0,
                        "st_atime": common.timestamp(),
                        "type":     finfo["type"]
                    }
            self.fileprops[path_as_key] = st_info

        allfiles = ['.','..']

        for remotefile in metadata_for_dir["files"]:
            allfiles.append(remotefile["name"])

        self.rootfiles = allfiles
        return allfiles

    def readdir(self, path, fh):
        # import pdb;pdb.set_trace()
        return self.rootfiles

    def getattr(self, path, fh=None):
        ''' 'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid' '''
        if path in self.fileprops:
            return self.fileprops[path]
        else:
            raise FuseOSError(ENOENT)

    def read(self, path, size, offset, fh):
        # import pdb;pdb.set_trace()
        try:
            hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
            downloadtask = self.taskpool.query_download_task(hashpath)
            if downloadtask:
                #等待数据
                logger.debug("再次进入等待下载的数据")
                return downloadtask.wait_data(size)
            else:
                logger.debug("创建下载任务, require %d bytes" % size)
                downloadtask = self.taskpool.download_file(hashpath, self.api, path)
                return downloadtask.wait_data(size)

        except Exception ,e:
            logger.error(e)
            import traceback;traceback.print_exc()

    def unlink(self, path):
        result = self.api.delete(path)
        if result.status_code == 200:
            logger.debug("%s delete ok" % path)
            if self.fileprops.has_key(path):
                self.listdir(os.path.dirname(path))
                del self.fileprops[path]
            return 0
        else:
            logger.error("%s delete failed, reason is:%s" % (path,result.text))
            raise FuseOSError(ENOENT)

    def truncate(self, path, length, fh=None):
        if self.fileprops.has_key(path):
            raise FuseOSError(EEXIST)

    def rename(self, oldpath, newpath):
        result = self.api.move(oldpath, newpath)
        if result.status_code == 200:
            oldinfo = self.fileprops[oldpath]
            self.fileprops[newpath] = oldinfo
            del self.fileprops[oldpath]
        else:
            raise FuseOSError(EIO)

    def __isdir(self, path):
        return self.fileprops[path]["st_mode"] == TYPE_DIR

    def __isfile(self, path):
        return self.fileprops[path]["st_mode"] == TYPE_FILE

    def access(self, path, mode):

        if self. __isdir(path):
            self.listdir(path)
            return 0

        if path not in self.fileprops:
            self.listdir(path)
            if path in self.fileprops:
                return 0
            else:
                raise FuseOSError(ENOENT)
            return 0
        elif path in self.fileprops:
            return 0
        else:
            raise FuseOSError(EROFS)

    def __getparentdir(self, path):
        return os.path.abspath(os.path.join(path, os.pardir))

    def rmdir(self, path):

        if self.fileprops.has_key(path):
            if len(
                    filter(
                        lambda dirname:dirname.startswith(path), self.fileprops.keys())
                    ) > 1:
               raise FuseOSError(ENOTEMPTY)
            else:
               del self.fileprops[path]

        result = self.api.delete(path)
        if result.status_code == 200:
            self.listdir(self.__getparentdir(path))
            return 0
        else:
            raise FuseOSError(ENOENT)

    def mkdir(self, path, mode):
        result = self.api.create_folder(path)
        if result.status_code == 200:
            self.fileprops[path] = ROOT_ST_INFO
        else:
            logger.debug("mkdir %s failed" % path)

    def create(self, path, mode, fi=None):
        fileinfo = copy.copy(ROOT_ST_INFO)
        fileinfo["st_mode"] = (S_IFREG | mode)
        fileinfo["st_nlink"] = 1
        self.fileprops[path] = fileinfo
        self.fd += 1
        return self.fd

    def write(self, path, data, offset, ph):
        try:
            hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
            filename = os.path.basename(path)
            writetask = self.taskpool.query_upload_task(hashpath)
            if writetask:
                writetask.push_upload_file(data, offset)
            else:
                self.taskpool.upload_file(hashpath, self.api, path, filename, data, offset)
            return len(data)
        except Exception, e:
            logger.error(e)
            raise FuseOSError(ENOENT)
        finally:
            pass


    def release(self, path, fh):
        '''
        '''
        hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
        uploadtask = self.taskpool.query_upload_task(hashpath)
        if uploadtask:
            uploadtask.end_upload_file()

        downloadtask = self.taskpool.query_download_task(hashpath)
        if downloadtask:
            downloadtask.end_download_file()

        return 0

    def destroy(self, path):
        pass

consumer_key = 'xcr5NKAtWRtjWVOb'
consumer_secret = 'mPM0J4vn6gDu3oJG'

def main(consumer_key, consumer_secret, app_name, username, userpwd, url):
    api = KuaipanAPI(consumer_key, consumer_secret)
    api.request_token()
    authorize_url = api.authorize()
    auth_code = api.get_auth_code(authorize_url)
    api.access_token(auth_code)
    print api.account_info()

if __name__ == "__main__":

    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    fuse = FUSE(KuaiPanFuse(), argv[1], foreground=False, nothreads=False, debug=False)

