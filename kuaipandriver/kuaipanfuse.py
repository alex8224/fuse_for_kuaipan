#!/usr/bin/python
# -*-coding:utf-8 -*-

#********************************************************************************
#Author: tony - birdaccp@gmail.com alex8224@gmail.com
#Create by: 2013-08-17 12:53
#Last modified: 2014-07-01
#Filename: kuanpanfuse.py
#Description: 实现快盘的ＡＰＩ,可以实现文件的异步上传, 下载，浏览，目录操作
# 缩略图和版本功能未实现
#todo
#1.download
#2. retry after failed
#********************************************************************************

import os
import sys
import time
import copy
import signal
import common
import logging
import traceback
from hashlib import sha1
from functools import partial
from Queue import Queue, Empty
from stat import S_IFDIR, S_IFREG
from kuaipanapi import OpenAPIError, OpenAPIException
from threading import Thread, Event,  Condition, RLock as Lock
from errno import ENOENT,EROFS, EEXIST, EIO
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


TYPE_DIR= (S_IFDIR | 0644 )
TYPE_FILE= (S_IFREG | 0644 )
BLOCK_SIZE = 4096

CACHE_PATH = "/dev/shm/"

ROOT_ST_INFO = {
        "st_mtime": common.timestamp(),
        "st_mode":  TYPE_DIR,
        "st_size":  4096,
        "st_gid":   os.getgid(),
        "st_uid":   os.getuid(),
        "st_atime": common.timestamp(),
        "st_nlink": 2
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
    logger.setLevel(logging.WARN)
    return logger

logger = getLogger()

def handler(signum, frame):
    print "use press ctrl+c exit"
    sys.exit(0)

signal.signal(signal.SIGINT, handler)

def unique_id():
    from hashlib import md5
    from uuid import uuid1
    return md5(str(uuid1())).hexdigest()

class Future(object):

    def __init__(self, notify = None):
        self.result = None
        self.finished = False
        self.notify = notify
        self.waitobj = Condition()

    def get(self):
        while 1:
            with self.waitobj:
                if self.is_finished():
                    self.notify(self.result)
                    return self.result
                else:
                    self.waitobj.wait()

    def set_result(self, result):
        with self.waitobj:
            self.result = result
            self.finished = True
            self.waitobj.notify()

    def is_finished(self):
        return self.finished


class TaskPool(object):
    def __init__(self):
        self.uploadpool = {}
        self.downloadpool = {}
        self.uploadlock, self.downlock = Lock(), Lock()
        self.taskclass = {"upload":WriteTask,"download":DownloadTask, "fusetask":FuseTask}

    def upload_file(self, hashpath, *args):
        return self.__new_task("upload", hashpath, *args)

    def download_file(self, hashpath, *args):
        return self.__new_task("download", hashpath, *args)

    def do_fuse_task(self, method, *args, **kwargs):
        return self.__new_task("fusetask", unique_id(), method, *args, **kwargs)

    def do_delay_task(self, callableobj):
        '''执行周期性的任务, 执行完就会从队列中删除, 在任务执行之前，调用者可以选择终止该任务，　任务可以被重新加入队列'''
        pass

    def __task_sucess(self, tasktype, key, result=None, callback=None):
        lock = self.__getlock(tasktype)
        with lock:
            logger.debug("task %s already complted!, delete it!" % key)
            pool = self.__getpool(tasktype)
            if callback:
                logger.debug("call callback method, the result is:%s" % str(result))
                callback(result)

            if pool.has_key(key):
                del pool[key]

    def __getlock(self, tasktype):
        return self.uploadlock if tasktype == "upload" else self.downlock

    def __getpool(self, tasktype):
        return self.uploadpool if tasktype == "upload" else self.downloadpool

    def __new_task(self, tasktype, key, *args, **kwargs):
        if kwargs and "callback" in kwargs:
            callback_wrapper = partial(self.__task_sucess, tasktype, key, callback=kwargs["callback"])
        else:
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

class PeriodicTask(Thread, Future):

    def __init__(self):
        self.lock = Lock()
        self.id2task= {}

    def delay(self, callableobj, timeout):

        with self.lock:
            taskid = unique_id()
            task = (time.time(), callableobj, timeout)
            self.id2task[taskid] = task
            return taskid

    def cancel(self, taskid):
        with self.lock:
            if taskid in self.id2task:
                self.task2id.pop(taskid)
                logger.debug("task %s already canceled!")

    def run(self):

        cleanedtask = []
        while 1:
            with self.lock:
                for taskid, task in self.id2task.iteritems():
                    addtime, callableobj, timeout = task
                    if (time.time() - addtime) > timeout:
                        logger.debug("execute task %s")
                        callableobj()
                        cleanedtask.append(taskid)

                for taskid in cleanedtask:
                    del self.id2task[taskid]
                cleanedtask = []

            time.sleep(0.01)

class FuseTask(Thread, Future):

    def __init__(self, method, key, api, *args):
        Thread.__init__(self)
        Future.__init__(self)
        self.method = method
        self.api = api
        self.args = args[:-1]
        self.notify = args[-1:][0]


    def readdir(self, path):
        return self.api.metadata(path=path)

    def mkdir(self, path):
        return self.api.create_folder(path)

    def rmdir(self, path):
        return self.api.delete(path)

    def unlink(self, path):
        return self.rmdir(path)

    def rename(self, oldpath, newpath):
        return self.api.move(oldpath, newpath)

    def accountinfo(self):
        return self.api.account_info()

    def run(self):
        try:
            method = getattr(self, self.method)
            result = method(*self.args)
            logger.debug(self.method + " - " + result.text)
            if result.status_code == 200:
                self.set_result((True, result.json()))
            else:
                self.set_result((False,FuseOSError(EIO)))
            self.notify(self.result) 
        except Exception ,e:
            _, exc, trace = sys.exc_info()
            logger.error(exc)
            self.set_result = FuseOSError(EIO)

class DownloadTask(Thread, Future):

    def __init__(self, api, hashpath, path, bufsize, notify):
        Thread.__init__(self)
        Future.__init__(self)
        self.api = api
        self.path = path
        self.hashpath = hashpath
        self.notify = notify
        self.waiter = Event()
        self.isfirst = True
        self.bufsize = bufsize
        self.downloadbytes = 0
        self.lock = Lock()

    def wait_data(self,size):
        self.waiter.wait()
        with self.lock:
            if self.isfirst:
                self.buffgen.send(None)
                firstdata = self.buffgen.send(self.bufsize)
                self.downloadbytes += len(firstdata)
                self.isfirst = False
                return firstdata
            else:
                try:
                    data = self.buffgen.send(size)
                    self.downloadbytes += len(data)
                    return data
                except StopIteration:
                    logger.error("gen stoped============================")
                    self.buffgen.close()
                except Exception, e:
                    traceback.print_exc()

    def end_download_file(self):
        logger.debug("file %s download completed, %d bytes downloaded, key %s" % (self.path, self.downloadbytes, self.hashpath))
        with self.lock:
            self.notify()

    def run(self):
        logger.debug("start download %s" % self.path)
        with self.lock:
            self.buffgen = self.api.download_file(self.path, self.bufsize)
            self.waiter.set()

class WriteTask(Thread, Future):

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
        if os.path.exists(self.fullpath):
            os.unlink(self.fullpath)

        with open(self.fullpath, "w") as f:
            f.seek(offset)
            f.write(data)
            self.writebytes += len(data)

    def push_upload_file(self, data, offset):
        self.sendmesg(("push_upload_file", (data, offset)))

    def __push_upload_file(self, data, offset):
        if os.path.exists(self.fullpath):
            with open(self.fullpath, "a") as f:
                f.seek(offset)
                f.write(data)
                self.writebytes += len(data)

    def end_upload_file(self):
        self.sendmesg(("end_upload_file", ()))

    def __end_upload_file(self):
        def upload_file():
            try:
                logger.debug("start upload file %s to kuaipan server" % self.path)
                uploadpath = os.path.dirname(self.path)
                uploadresult = self.api.upload(uploadpath, self.fullpath, self.filename)
                if uploadresult.status_code == 200:
                    logger.debug("file %s upload ok" % self.filename)
                    return True
                else:
                    logger.error("file %s upload failed!" % self.filename)
                    return False
            except OpenAPIError, apiex:
                logger.error(apiex)
        try:
            for retry in range(10):
                if retry > 0:
                    logger.error("retry %d upload_file %s" % (retry, self.filename))
                if upload_file():
                    break
                else:
                    time.sleep(30)
        except Exception, e:
            logger.error("file %s cannot be uploaded, the reason is that %s, please upload this file by manual" % (self.filename,str(e)))
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

    def __init__(self, api):
        self.api = api
        self.root = "."
        self.fd = 0
        self.fileprops = {"/":ROOT_ST_INFO}
        self.taskpool = TaskPool()
        self.rootfiles = []
        self.cwd = ''
        self.rlock = Lock()
        self.setaccountinfo()
        self.quota = {"f_blocks":0, "f_bavail":0}

    def __call__(self, op, *args):
        try:
            return super(KuaiPanFuse, self).__call__(op, *args)
        except Exception ,e:
            if isinstance(e, OpenAPIError) or isinstance(e, OpenAPIException):
                raise FuseOSError(EIO)
            else:
                raise

    def setaccountinfo(self):

        def updatediskquota(result):
            _, quota = result
            totalspace, usedspace = quota["quota_total"], quota["quota_used"]
            f_blocks = totalspace / BLOCK_SIZE
            availspace = totalspace - usedspace
            f_bavail = availspace / BLOCK_SIZE
            self.quota["f_blocks"] = int(f_blocks)
            self.quota["f_bavail"] = int(f_bavail)
            logger.debug("totalspace=%d, usedspace=%d" % (totalspace, usedspace))

        self.taskpool.do_fuse_task("accountinfo", self.api, callback=updatediskquota)


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
        with self.rlock:
            task = self.taskpool.do_fuse_task("readdir", self.api, path)
            success, result = task.get()
            if not success: raise result
            self.cwd = path
            for finfo in result["files"]:
                path_as_key = path + finfo["name"] if path == "/" else path + "/" + finfo["name"]
                st_info = {
                            "st_mtime": common.to_timestamp(finfo["modify_time"]),
                            "st_mode":  TYPE_FILE if finfo["type"] == "file" else TYPE_DIR,
                            "st_size":  int(finfo["size"]) if finfo["type"] == "file" else 4096,
                            "st_gid":   os.getgid(),
                            "st_uid":   os.getuid(),
                            "st_atime": common.timestamp(),
                        }
                self.fileprops[path_as_key] = st_info

            allfiles = ['.','..']

            for remotefile in result["files"]:
                allfiles.append(remotefile["name"])

            self.rootfiles = allfiles
            return allfiles

    def chmod(self, path, mode):
        logger.debug("chmod %s" % path)

    def readdir(self, path, fh):
        with self.rlock:
            if self.rootfiles:
                logger.debug("return self.rootfiles")
                logger.debug(self.rootfiles)
                logger.debug(self.fileprops)
                return self.rootfiles
            else:
                logger.debug("return default files")
                logger.debug(self.fileprops)
                return ['.','..']

    def getattr(self, path, fh=None):
        ''' 'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid' '''
        if path in self.fileprops:
            return self.fileprops[path]
        else:
            raise FuseOSError(ENOENT)

    def read(self, path, size, offset, fh):
        hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
        downloadtask = self.taskpool.query_download_task(hashpath)
        if downloadtask:
            logger.debug("ReEnter wait_data for file %s , require %d bytes" % (path,size))
            return downloadtask.wait_data(size)
        else:
            logger.debug("Create download task%s, file %s, require %d bytes" % (hashpath, path, size))
            downloadtask = self.taskpool.download_file(hashpath, self.api, path, size)
            return downloadtask.wait_data(size)

    def unlink(self, path):
        self.taskpool.do_fuse_task("unlink", self.api, path)
        self.__cleancache(path)
        return 0

    def truncate(self, path, length, fh=None):
        if self.fileprops.has_key(path):
            raise FuseOSError(EEXIST)

    def rename(self, oldpath, newpath):
        with self.rlock:
            self.taskpool.do_fuse_task("rename", self.api, oldpath, newpath)
            oldinfo = self.fileprops[oldpath]
            self.fileprops[newpath] = oldinfo
            del self.fileprops[oldpath]

    def __isdir(self, path):
        return self.fileprops[path]["st_mode"] == TYPE_DIR

    def __isfile(self, path):
        return self.fileprops[path]["st_mode"] == TYPE_FILE

    def access(self, path, mode):
        if self.__isdir(path):
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

    def __getcurrentdir(self, path):
        return os.path.dirname(path)

    def __fileexists(self, path):
        return self.rootfiles.index(path) > -1

    def __cleancache(self, path):
        with self.rlock:
            logger.debug("delete %s from cache" % path)
            allpath = filter(lambda f:f.startswith(path), self.fileprops.iterkeys())
            for path2clean in allpath:
                logger.debug("delete %s" % path2clean)
                del self.fileprops[path2clean]

            pure_file = os.path.basename(path)
            if pure_file in self.rootfiles:
                fileindex = self.rootfiles.index(pure_file)
                del self.rootfiles[fileindex]

    def __addcache(self, path, typecode):
        logger.debug("add %s to cache" % path)
        with self.rlock:
            if typecode == TYPE_FILE:
                fileinfo = copy.copy(ROOT_ST_INFO)
                fileinfo["st_mode"] = TYPE_FILE
                fileinfo["st_nlink"] = 1
                self.fileprops[path] = fileinfo
                dirname = os.path.dirname(path)
                if dirname == self.cwd:
                    self.rootfiles.append(os.path.basename(path))
            else:
                dirinfo = copy.copy(ROOT_ST_INFO)
                self.fileprops[path] = dirinfo
                self.rootfiles.append("".join(path[1:]))


    def __updatefilesize(self, path, size):
        with self.rlock:
            if self.fileprops.has_key(path):
                finfo = self.fileprops[path]
                finfo["st_size"] = size
                logger.debug("update %s size to %d" % (path, size))

                # filename = os.path.basename(path)
                # dirname = os.path.dirname(path)
                # if dirname == self.cwd:
                    # self.__addcache(path, TYPE_FILE)

    def rmdir(self, path):
        self.taskpool.do_fuse_task("rmdir", self.api, path)
        self.__cleancache(path)
        return 0

    def mkdir(self, path, mode):
        self.taskpool.do_fuse_task("mkdir", self.api, path)
        self.__addcache(path, TYPE_DIR)

    def link(self, source ,target):
        return 0

    def create(self, path, mode, fi=None):
        self.__addcache(path, TYPE_FILE)
        self.fd += 1
        return self.fd

    def write(self, path, data, offset, ph):
        hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
        filename = os.path.basename(path)
        writetask = self.taskpool.query_upload_task(hashpath)
        if writetask:
            writetask.push_upload_file(data, offset)
        else:
            self.taskpool.upload_file(hashpath, self.api, path, filename, data, offset)
        return len(data)


    def release(self, path, fh):
        hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
        uploadtask = self.taskpool.query_upload_task(hashpath)
        if uploadtask:
            uploadtask.end_upload_file()
            filesize = uploadtask.writebytes
            self.__updatefilesize(path, filesize)

        downloadtask = self.taskpool.query_download_task(hashpath)
        if downloadtask:
            downloadtask.end_download_file()
        return 0

    def statfs(self, path):
        return dict(
                    f_bsize=BLOCK_SIZE, 
                    f_frsize=BLOCK_SIZE, 
                    f_blocks=self.quota["f_blocks"], 
                    f_bfree=self.quota["f_bavail"], 
                    f_bavail=self.quota["f_blocks"]
                    )
        

    def destroy(self, path):
        pass


def main():
    from kuaipandriver.kuaipanapi import KuaipanAPI
    from kuaipandriver.common import getauthinfo
    from requests.exceptions import RequestException
    while 1:
        try:
            mntpoint, key, secret, user, pwd = getauthinfo()
            api = KuaipanAPI(mntpoint, key, secret, user, pwd)
            break
        except RequestException:
            retry = raw_input("Your Login info already wrong, do you want to re enter it?(Y/N)")
            if retry == "Y":
                from kuaipandriver.common import deleteloginfo
                deleteloginfo()
            else:
                break

    try:
        FUSE(
                KuaiPanFuse(api), mntpoint, foreground=False, nothreads=False, 
                debug=False, direct_io=False, gid=os.getgid(), uid=os.getuid(), 
                umask='0133'
            )
    except RuntimeError as err:
        print str(err)
