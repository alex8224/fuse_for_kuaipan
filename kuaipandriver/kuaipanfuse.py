#!/usr/bin/python
# -*-coding:utf-8 -*-

#********************************************************************************
#Author: tony - birdaccp@gmail.com alex8224@gmail.com
#Create by: 2013-08-17 12:53
#Last modified: 2014-07-01
#Filename: kuanpanfuse.py
#Description: 实现快盘的ＡＰＩ,可以实现文件的异步上传, 下载，浏览，目录操作
# 缩略图和版本功能未实现
# todo
# 1. bulid entire director tree
# from root get file list. 判断如果是目录，则继续调用ＡＰＩ，轮询该目录,如果该目录下没有其他目录，则返回上一级
# 使用stack代替递归，how?
# 2. make wrietask and download task managmented by threadpool
# 3. add virtualdirectory support extend task, for example: document convert
#********************************************************************************
import os
import sys
import time
import copy
import signal
import common
import traceback
from hashlib import sha1
from functools import partial
from collections import deque
from Queue import Queue, Empty
from stat import S_IFDIR, S_IFREG
from kuaipanapi import OpenAPIError, OpenAPIException
from threading import Thread, Event,  Condition, RLock as Lock
from errno import ENOENT, EROFS, EEXIST, EIO
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

TYPE_DIR = (S_IFDIR | 0644 )
TYPE_FILE = (S_IFREG | 0644 )
BLOCK_SIZE = 4096

CACHE_PATH = "/dev/shm/"

ROOT_ST_INFO = {
        "st_mtime": common.timestamp(),
        "st_mode":  TYPE_DIR,
        "st_size":  4096,
        "st_gid":   os.getgid(),
        "st_uid":   os.getuid(),
        "st_atime": common.timestamp(),
        "st_nlink": 2,
        "st_parent_dir":".",
        "st_name": "/"
        }

import logging
def getlogger():
    logger = logging.getLogger("kuaipanserver")
    hdlr = logging.StreamHandler()
    fdlr = logging.handlers.TimedRotatingFileHandler("kuaipan_fuse.log", 'D', backupCount=30)
    format = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    logger.addHandler(hdlr)
    logger.addHandler(fdlr)
    hdlr.setFormatter(format)
    fdlr.setFormatter(format)
    logger.setLevel(logging.DEBUG)
    return logger


logger = getlogger()

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


class ThreadPool(Thread):

    _instance_lock = Lock()

    def __init__(self):
        Thread.__init__(self)
        from config import Config
        configobj = Config()
        self.max_thread_num = int(configobj.get_max_thread_num())
        self.free_thread_num = int(configobj.get_free_thread_num())
        self.idle_seconds = int(configobj.get_idle_seconds())
        self.runflag = False
        self.freequeue = deque()
        self.busyqueue = deque()
        self.delayqueue = deque()
        self.lock = Lock()
        self.condition = Condition(self.lock)
        self.setDaemon(True)
        self.initpool()

    @staticmethod
    def instance():
        '''create singleton ThreadPool object'''
        if not hasattr(ThreadPool, "_instance"):
            with ThreadPool._instance_lock:
                if not hasattr(ThreadPool, "_instance"):
                    ThreadPool._instance = ThreadPool()
        return ThreadPool._instance


    def initpool(self):
        for workerindex in range(self.free_thread_num):
            self.__createworker()

    def quit(self):
        '''wait for all busy task done'''''
        while 1:
            with self.condition:
                if len(self.busyqueue) > 0:
                    logger.debug("wait quit......")
                    self.condition.wait()
                else:
                    break

        for task in self.freequeue:
            task.quit()
            task.waitdone()
            
        with self.lock:
            self.runflag = False    

    def __createworker(self):
        worker = Worker()
        self.freequeue.append(worker)
        return worker

    def __dotaskinworker(self, callable_, *args, **kwargs):
        worker = self.freequeue.popleft()
        self.busyqueue.append(worker)
        worker.execute(callable_, *args, **kwargs)
        return self.future(callable_)

    def delay(self, callable_, timeout):
        '''schedule a delay task'''
        with self.lock:
            self.delayqueue.append((time.time(), timeout, callable_))

    def peroidic(self, callable_, interval):
        '''schedual a peroidic task'''
        def _peroidicwrapper():
            self.delay(callable_, interval)

        with self.lock:
            _peroidicwrapper.callablefunc = callable_
            self.delayqueue.append((time.time(), interval, _peroidicwrapper))

    def future(self, callable_):
        '''get future result'''
        if issubclass(callable_.__class__, Future):
            return callable_

    def submit(self, callable_, *args, **kwargs):
        if not callable(callable_):
            raise TypeError("%s not a callable object" % str(callable_))

        with self.lock:

            idle_threadsize = len(self.freequeue)
            busy_threadsize = len(self.busyqueue)
            all_threadsize = idle_threadsize + busy_threadsize

            if idle_threadsize > 0:
                return self.__dotaskinworker(callable_, *args, **kwargs)

            elif idle_threadsize == 0 and all_threadsize < self.max_thread_num:
                self.__createworker()
                return self.__dotaskinworker(callable_, *args, **kwargs)

            elif idle_threadsize == 0 and all_threadsize == self.max_thread_num:
                logger.debug("no available thread, wait for notify...")
                self.condition.wait()
                return self.__dotaskinworker(callable_, *args, **kwargs)

    def taskdone(self, worker):
        with self.condition:
            self.condition.notify()
            logger.debug("worker:(%s) task done" % str(worker))
            self.freequeue.append(worker)
            self.busyqueue.remove(worker)
            

    def run(self):
        lastcleantime = time.time()
        self.runflag = True
        while self.runflag:
            with self.lock:
                if time.time() - lastcleantime > self.idle_seconds:
                    needcleantasknum = len(self.freequeue) - self.free_thread_num
                    for workerindex in range(needcleantasknum):
                        task = self.freequeue.pop()
                        task.quit()
                    
                    lastcleantime = time.time()

                delaytasks = filter(lambda task: time.time() - task[0] > task[1], self.delayqueue)
                for delaytask in delaytasks:
                    callable_ = delaytask[2]
                    timeoutvalue = delaytask[1]
                    callable_()
                    self.delayqueue.remove(delaytask)
                    if hasattr(callable_, "callablefunc"):
                        self.peroidic(callable_.callablefunc, timeoutvalue)

            time.sleep(0.1)
        logger.debug("threadpool exited!")


class Worker(Thread, Future):
    def __init__(self):
        Thread.__init__(self)
        Future.__init__(self)
        self.status = "FREE"
        self.taskqueue = Queue(2)
        self.lastupdate = time.time()
        self.notify = lambda noop:noop
        self.setDaemon(True)
        self.start()

    def execute(self, callable_, *args, **kwargs):
        self.status = "BUSY"
        taskinfo = ("DO_TASK", callable_, args, kwargs)
        self.taskqueue.put(taskinfo)

    def quit(self):
        self.taskqueue.put(("QUIT", None, None, None))

    def waitdone(self):
        while 1:
            with self.waitobj:
                if self.status == "QUIT":
                    break
                else:
                    self.waitobj.wait()

    def run(self):
        while 1:
            cmd, task, args, kwargs = self.taskqueue.get()
            if cmd == "QUIT":
                self.status = "QUIT"
                self.lastupdate = time.time()
                break
            else:
                try:
                    self.status = "RUNNING"
                    result = task(*args, **kwargs)
                    task.set_result(result)
                    self.status = "FREE"
                    self.lastupdate = time.time()
                except Exception, taskex:
                    task.set_result(result)
                    self.status = "FREE"
                    logger.error(taskex)
                finally:
                    ThreadPool.instance().taskdone(self)
                    

class TaskPool(object):
    def __init__(self):
        self.uploadpool = {}
        self.downloadpool = {}
        self.uploadlock, self.downlock = Lock(), Lock()
        self.taskclass = {"upload":WriteTask, "download":DownloadTask, "fusetask":FuseTask}

    def upload_file(self, hashpath, *args):
        return self.__new_task("upload", hashpath, *args)

    def download_file(self, hashpath, *args):
        return self.__new_task("download", hashpath, *args)

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

class WalkAroundTreeTask(Future):

    def __init__(self, api, path):
        Future.__init__(self)
        self.api = api
        self.path = path

    def readdir(self, path):
        return self.api.metadata(path=path)

    def listdir(self, path, allfilesinfo):
        dirs = []
        result = self.readdir(path)
        if result.status_code != 200:
            return dirs

        result = result.json()
        for finfo in result["files"]:
            fullpath = os.path.join(path, finfo["name"])

            st_info = {
                        "st_mtime": common.to_timestamp(finfo["modify_time"]),
                        "st_mode":  TYPE_FILE if finfo["type"] == "file" else TYPE_DIR,
                        "st_size":  int(finfo["size"]) if finfo["type"] == "file" else 4096,
                        "st_gid":   os.getgid(),
                        "st_uid":   os.getuid(),
                        "st_atime": common.timestamp(),
                        "st_parent_dir": path,
                        "st_name": finfo["name"]
                    }

            allfilesinfo[fullpath] = st_info

            print("%s" % fullpath)
            if finfo["type"] == "file":
                dirs.append((fullpath, "file"))
            else:
                dirs.append((fullpath, "dir"))
        return dirs        

    def walk(self, root):
        stack = []
        stack.append(root)
        allfilesinfo = {}

        while 1:
            if len(stack) == 0:
                break
            cwd = stack.pop(0)
            try:
                files = self.listdir(cwd, allfilesinfo)
                for fileinfo in files:
                    name, nodetype = fileinfo
                    path = os.path.join(cwd, name)

                    if nodetype == "dir":
                        stack.append(path)
            except:
                pass
        return allfilesinfo

    def __call__(self, *args):
        return self.walk(self.path)                

class FuseTask(Future):
    def __init__(self, method, api, *args, **kwargs):
        Future.__init__(self)
        self.method = method
        self.api = api
        self.args = args
        self.kwargs = kwargs

    def __call__(self, *args):
        try:
            method = getattr(self, self.method)
            result = method(*self.args)
            logger.debug(self.method + " - " + result.text)
            if result.status_code == 200:
                logger.debug("method %s" % self.method)
                return (True, result.json())
            else:
                return (False, FuseOSError(EIO))
        except Exception:
            _, exc, _ = sys.exc_info()
            logger.error(exc)
            return FuseOSError(EIO)

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


class DownloadTask(Thread, Future):

    def __init__(self, api, hashpath, path, bufsize, notify):
        Thread.__init__(self)
        Future.__init__(self)
        self.api = api
        self.path = path
        self.hashpath = hashpath
        self.waiter = Event()
        self.isfirst = True
        self.bufsize = bufsize
        self.notify = notify
        self.downloadbytes = 0
        self.lock = Lock()
        self.waitobj = Condition(self.lock)

    def wait_data(self, size):
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
            self.buffgen.close()

    def run(self):
        logger.debug("start download %s" % self.path)
        with self.lock:
            self.buffgen = self.api.download_file(self.path, self.bufsize)
            self.waiter.set()

class WriteTask(Thread, Future):

    def __init__(self, api, hashpath, path, filename, data, offset, uploadqueue, notify):
        Thread.__init__(self)
        Future.__init__(self)
        self.api = api
        self.hashpath = hashpath
        self.path = path
        self.filename = filename
        self.fullpath = CACHE_PATH + hashpath
        self.writebytes = 0
        self.filesize = 0
        self.cmd = Queue(1000)
        self.cmd.put(("start_upload_file",(data, offset)))
        self.notify = notify
        self.uploadqueue = uploadqueue
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

    def push_upload_file(self, data, offset):
        self.sendmesg(("push_upload_file", (data, offset)))

    def __push_upload_file(self, data, offset):
        if os.path.exists(self.fullpath):
            with open(self.fullpath, "a") as f:
                f.seek(offset)
                f.write(data)

    def end_upload_file(self):
        self.sendmesg(("end_upload_file", ()))

    def __end_upload_file(self):
        def upload_file():
            try:
                logger.debug("start upload file %s to kuaipan server" % self.path)
                self.filesize = os.stat(self.fullpath)[6]
                uploadpath = os.path.dirname(self.path)
                uploadresult = self.api.upload(uploadpath, self.fullpath, self.filename)
                if uploadresult:
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
            import traceback;traceback.print_exc()
            logger.error("file %s upload failed, the reason is %s, please upload this file manual" % (self.filename, str(e)))
        finally:
            self.sendmesg("quit")
            self.is_finished = True
            self.notify(self.result)
            self.__dropcache()
            if self.hashpath in self.uploadqueue:
                del self.uploadqueue[self.hashpath]

    def __dropcache(self):
        try:
            os.unlink(self.fullpath)
        except Exception, e:
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
        self.quota = {"f_blocks":0, "f_bavail":0}
        self.walked = False
        self.initkuaipan()
        #用来保存尚未上传完成的文件,如果上传完后，需要从这里删除上传完成的文件
        self.uploadqueue = {}

    def initkuaipan(self):
        localtree = self.loadtree()

        logger.debug(localtree)
        if localtree: 
            self.fileprops = localtree
        threadpool = ThreadPool.instance()
        threadpool.delay(self.setaccountinfo, 0)
        threadpool.peroidic(self.setaccountinfo, 300)
        threadpool.delay(self.updatefileprops, 10)

        
    def __call__(self, op, *args):
        try:
            return super(KuaiPanFuse, self).__call__(op, *args)
        except Exception, e:
            if isinstance(e, OpenAPIError) or isinstance(e, OpenAPIException):
                raise FuseOSError(EIO)
            else:
                raise

    def setaccountinfo(self):

        def updatediskquota(result):
            try:
                _, quota = result
                totalspace, usedspace = quota["quota_total"], quota["quota_used"]
                availspace = totalspace - usedspace
                self.quota["f_blocks"] = totalspace / BLOCK_SIZE
                self.quota["f_bavail"] = availspace / BLOCK_SIZE
                logger.debug("totalspace=%d, usedspace=%d" % (totalspace, usedspace))
            except:
                pass

        future = ThreadPool.instance().submit(FuseTask("accountinfo", self.api))
        result = future.get()
        logger.debug(result)
        updatediskquota(result)

    def updatefileprops(self):
        with self.rlock:
            future = ThreadPool.instance().submit(WalkAroundTreeTask(self.api, "/"))
            allfilesinfo = future.get()
            allfilesinfo.update({"/":ROOT_ST_INFO})
            self.fileprops = allfilesinfo

    def listdir(self, path=None):
        if self.walked:
            return 
        else:
            self.walked = True

    def chmod(self, path, mode):
        logger.debug("chmod %s" % path)

    def __listfiles(self, path):
        allfiles = ['.','..']
        for remotefile in filter(lambda node:node["st_parent_dir"] == path and node["st_name"] != '/', self.fileprops.itervalues()):
            allfiles.append(remotefile["st_name"])
        return allfiles

    def readdir(self, path, fh):
        return self.__listfiles(path)

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
            logger.debug("ReEnter wait_data for file %s , require %d bytes" % (path, size))
            return downloadtask.wait_data(size)
        else:
            logger.debug("Create download task%s, file %s, require %d bytes" % (hashpath, path, size))
            downloadtask = self.taskpool.download_file(hashpath, self.api, path, size)
            return downloadtask.wait_data(size)

    def link(self, linktarget, linkname):
        self.__waituploadready(linkname)
        ThreadPool.instance().submit(FuseTask("rename", self.api, linkname, linktarget))
        with self.rlock:
            oldinfo = self.fileprops[linkname]
            self.fileprops[linktarget] = oldinfo
            self.__addcache(linktarget, oldinfo["st_mode"], size=oldinfo["st_size"])
            return 0

    def unlink(self, path):
        ThreadPool.instance().submit(FuseTask("unlink", self.api, path))
        self.__cleancache(path)
        return 0

    def truncate(self, path, length, fh=None):
        if self.fileprops.has_key(path):
            raise FuseOSError(EEXIST)

    def __waituploadready(self, path):
        hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
        while hashpath in self.uploadqueue:
            time.sleep(0.01)

    def rename(self, oldpath, newpath):
        with self.rlock:
            self.__waituploadready(oldpath)
            ThreadPool.instance().submit(FuseTask("rename", self.api, oldpath, newpath))
            oldinfo = self.fileprops[oldpath]
            self.fileprops[newpath] = oldinfo
            self.__addcache(newpath, oldinfo["st_mode"], size=oldinfo["st_size"])
            self.__cleancache(oldpath)

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
            return 
        else:
            raise FuseOSError(EROFS)


    def __cleancache(self, path):
        with self.rlock:
            allpath = filter(lambda f:f.startswith(path), self.fileprops.iterkeys())
            for path2clean in allpath:
                logger.debug("delete %s" % path2clean)
                del self.fileprops[path2clean]

            pure_file = os.path.basename(path)
            if pure_file in self.rootfiles:
                fileindex = self.rootfiles.index(pure_file)
                del self.rootfiles[fileindex]

    def __addcache(self, path, typecode, size=None):
        logger.debug("add %s to cache" % path)
        with self.rlock:
            if typecode == TYPE_FILE:
                fileinfo = copy.copy(ROOT_ST_INFO)
                fileinfo["st_mode"] = TYPE_FILE
                fileinfo["st_nlink"] = 1
                fileinfo["st_parent_dir"] = os.path.dirname(path)
                fileinfo["st_name"] = os.path.basename(path)
                if size:
                    fileinfo["st_size"] = size
                self.fileprops[path] = fileinfo
            else:
                dirinfo = copy.copy(ROOT_ST_INFO)
                dirinfo["st_parent_dir"] = os.path.dirname(path)
                dirinfo["st_name"] = os.path.basename(path)
                self.fileprops[path] = dirinfo


    def __updatefilesize(self, path, size):
        with self.rlock:
            if path in self.fileprops:
                finfo = self.fileprops[path]
                finfo["st_size"] = size
                logger.debug("update %s size to %d" % (path, size))

    def rmdir(self, path):
        '''delete directory not confirm'''
        ThreadPool.instance().submit(FuseTask("rmdir", self.api, path))
        self.__cleancache(path)
        return 0

    def mkdir(self, path, mode):
        '''create directory'''
        ThreadPool.instance().submit(FuseTask("mkdir", self.api, path))
        self.__addcache(path, TYPE_DIR)

    def create(self, path, mode, fi=None):
        '''called by fuse create new file'''
        logger.debug("create new file %s" % path)
        self.__addcache(path, TYPE_FILE)
        self.fd += 1
        return self.fd

    def write(self, path, data, offset, ph):
        '''called by fuse try to write data to path'''
        hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
        filename = os.path.basename(path)
        writetask = self.taskpool.query_upload_task(hashpath)
        if writetask:
            writetask.push_upload_file(data, offset)
        else:
            self.taskpool.upload_file(hashpath, self.api, path, filename, data, offset, self.uploadqueue)
            self.uploadqueue[hashpath] = path
        return len(data)


    def release(self, path, fh):
        '''called by fuse release a file'''
        hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
        uploadtask = self.taskpool.query_upload_task(hashpath)
        if uploadtask:
            uploadtask.end_upload_file()
            self.__waituploadready(path)
            self.__updatefilesize(path, uploadtask.filesize)

        downloadtask = self.taskpool.query_download_task(hashpath)
        if downloadtask:
            downloadtask.end_download_file()
        return 0

    def statfs(self, path):
        '''statvfs api'''
        logger.debug(self.quota)
        return dict(
                    f_bsize=BLOCK_SIZE, 
                    f_frsize=BLOCK_SIZE, 
                    f_blocks=self.quota["f_blocks"], 
                    f_bfree=self.quota["f_bavail"], 
                    f_bavail=self.quota["f_blocks"]
                    )
        
    def loadtree(self):
        '''load directory tree from local disk'''
        from pkg_resources import resource_filename
        filename = resource_filename("kuaipandriver", "tree.db")
        if os.path.exists(filename):
            from pickle import load
            return load(file(filename))

    def savetree(self):
        '''cache directory tree to local disk'''
        from pkg_resources import resource_filename
        filename = resource_filename("kuaipandriver", "tree.db")
        if not self.fileprops:
            pass
        with open(filename, "w") as treefile:
            from pickle import dump
            dump(self.fileprops, treefile)

    def destroy(self, path):
        '''called by fuse destory context'''
        logger.debug("fuse exited!")
        self.savetree()

def main():
    ThreadPool.instance().start()
    from kuaipandriver.kuaipanapi import KuaipanAPI
    from kuaipandriver.common import getauthinfo
    from requests.exceptions import RequestException
    islogin = False
    while 1:
        try:
            mntpoint, key, secret, user, pwd = getauthinfo()
            api = KuaipanAPI(mntpoint, key, secret, user, pwd)
            islogin = True
            break
        except RequestException:
            retry = raw_input("Your Login info already wrong, do you want to re enter it?(Y/N)")
            if retry == "Y":
                from kuaipandriver.common import deleteloginfo
                deleteloginfo()
            else:
                break

    try:
        if not islogin:
            return
        FUSE(
                KuaiPanFuse(api), mntpoint, foreground=False, nothreads=False, 
                debug=False, big_writes=True, gid=os.getgid(), uid=os.getuid(), 
                umask='0133'
            )
    except RuntimeError as err:
        print str(err)


if __name__ == '__main__':
    main()
