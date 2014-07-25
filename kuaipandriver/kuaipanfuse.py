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
# 1. add virtualdirectory support extend task, for example: document convert
# 2. support many consumerkey and consumersecret to solve api day limit
# 3. test host speed and using fastest connection download file
#********************************************************************************

import os
import sys
import time
import copy
import signal
import common
from common import HTTPSession, CopyOnWriteBuffer
from Queue import Queue
from hashlib import sha1
from functools import partial
from collections import deque
from stat import S_IFDIR, S_IFREG
from kuaipanapi import OpenAPIError, OpenAPIException
from threading import Thread, Condition, RLock as Lock
from errno import ENOENT, EROFS, EEXIST, EIO
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context

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
        return self.future(callable_, worker)

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

    def future(self, callable_, worker):
        '''get future result'''
        if issubclass(callable_.__class__, Future):
            return callable_
        else:
            return worker

    def submit(self, callable_, *args, **kwargs):

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

class Worker(Thread, Future):
    def __init__(self):
        Thread.__init__(self)
        Future.__init__(self)
        self.status = "FREE"
        self.taskqueue = Queue(2)
        self.lastupdate = time.time()
        self.notify = lambda noop:noop
        self.tasktype = "callable"
        self.setDaemon(True)
        self.start()
        self.interactivequeue = Queue()


    def sendmesg(self, methodname, mesg):
        self.interactivequeue.put((methodname,mesg))

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

    def runasinteractive(self, task):

        self.status = "RUNNING"
        while 1:
            methodname, args = self.interactivequeue.get()
            if methodname == "end_task":
                logger.debug("interactive terminated")
                self.status = "FREE"
                self.lastupdate = time.time()
                ThreadPool.instance().taskdone(self)
                break
            if hasattr(task, methodname):
                method = getattr(task, methodname)
                method(*args)
            else:
                logger.error("no such method %s" % methodname)

    def runascallable(self, task, *args, **kwargs):
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

        pass

    def run(self):
        while 1:
            cmd, task, args, kwargs = self.taskqueue.get()
            if cmd == "QUIT":
                self.status = "QUIT"
                self.lastupdate = time.time()
                break
            else:
                if callable(task):
                    self.runascallable(task, *args, **kwargs)
                else:
                    self.runasinteractive(task)
                    
                   
class TaskPool(object):
    def __init__(self):
        self.uploadpool = {}
        self.downloadpool = {}
        self.uploadlock, self.downlock = Lock(), Lock()
        self.taskclass = {"upload":WriteTask, "download":DownloadTask}

    def upload_file(self, hashpath, *args):
        params = list(args)
        params.insert(1, hashpath)
        task = ThreadPool.instance().submit(WriteTask(*params))
        self.uploadpool[hashpath] = task
        return task

    def query_upload_task1(self, key):
        if key in self.uploadpool:
            return self.uploadpool[key]

    def delete_upload_task(self, key):
        if key in self.uploadpool:
            del self.uploadpool[key]

    def delete_download_task(self, key):
        if key in self.downloadpool:
            del self.downloadpool[key]

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
        self.session = HTTPSession()

    def readdir(self, path):
        return self.api.metadata(path=path, session=self.session)

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
                import traceback;traceback.print_exc()

            
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


# class DownloadTask(Thread):
    # def __init__(self, api, hashpath, path, bufsize, notify):
        # Thread.__init__(self)
        # self.api = api
        # self.path = path
        # self.hashpath = hashpath
        # self.condi = Condition()
        # self.downloadbytes = 0
        # self.finished = False
        # import tempfile
        # self.tmpfilename = tempfile.mktemp()
        # self.continueread = True
        # self.setDaemon(True)

    # def wait_data(self, size, offset):
        # while self.continueread:
            # with self.condi:
                # if offset < self.downloadbytes:
                    # with open(self.tmpfilename) as ronlyfile:
                        # ronlyfile.seek(offset)
                        # return ronlyfile.read(size)
                # elif self.finished:
                    # break
                # else:
                    # self.condi.wait(5)
                    # logger.debug("%d:%d:%d:%s" % (offset, size, self.downloadbytes, self.tmpfilename))

    # def end_download_file(self):
        # with self.condi:
            # self.continueread = False
            # self.condi.notify_all()
            # logger.debug("clean cache %s" % self.tmpfilename)
            # if os.path.exists(self.tmpfilename):
                # os.unlink(self.tmpfilename)

    # def run(self):
        # self.stream = self.api.download_file1(self.path)
        # with open(self.tmpfilename, "w") as tempfile:
            # while 1:
                # data = self.stream.read(8192)
                # if not data:
                    # break
                # tempfile.write(data)
                # tempfile.flush()
                # with self.condi:
                    # self.downloadbytes += len(data)
                    # self.condi.notify_all()
        # self.stream.close()
        # with self.condi: 
            # self.finished = True
            # self.condi.notify_all()
        # logger.debug("%s download ok, hashpath:%s" % (self.path, self.hashpath))

# class DownloadTask(Thread, Future):

    # def __init__(self, api, hashpath, path, bufsize, notify):
        # Thread.__init__(self)
        # Future.__init__(self)
        # self.api = api
        # self.path = path
        # self.hashpath = hashpath
        # from threading import Event
        # self.waiter = Event()
        # self.isfirst = True
        # self.bufsize = bufsize
        # self.notify = notify
        # self.downloadbytes = 0
        # self.lock = Lock()
        # self.waitobj = Condition(self.lock)

    # def wait_data(self, size):
        # self.waiter.wait()
        # with self.lock:
            # if self.isfirst:
                # self.buffgen.send(None)
                # firstdata = self.buffgen.send(self.bufsize)
                # self.downloadbytes += len(firstdata)
                # self.isfirst = False
                # return firstdata
            # else:
                # try:
                    # data = self.buffgen.send(size)
                    # self.downloadbytes += len(data)
                    # return data
                # except StopIteration:
                    # logger.error("gen stoped============================")
                    # self.buffgen.close()
                # except Exception, e:
                    # import traceback
                    # traceback.print_exc()

    # def end_download_file(self):
        # logger.debug("file %s download completed, %d bytes downloaded, key %s" % (self.path, self.downloadbytes, self.hashpath))
        # with self.lock:
            # self.notify()
            # self.buffgen.close()

    # def run(self):
        # logger.debug("start download %s" % self.path)
        # with self.lock:
            # self.buffgen = self.api.download_file(self.path, self.bufsize)
            # self.waiter.set()

class DownloadTask(Thread):

    def __init__(self, api, hashpath, path, filesize, notify):
        Thread.__init__(self)
        self.api = api
        self.path = path
        self.stream = None
        self.filesize = filesize
        self.session = HTTPSession()
        self.condition = Condition()
        self.downloadurl = ''
        self.response = None
        self.initsession()

    def initsession(self):
        self.downloadurl = self.api.get_downloadurl(self.path)
        logger.debug("download url is %s" % self.downloadurl)

    def read_data(self, offset, size):
        with open(self.tmpfilename) as cachedfile:
            cachedfile.seek(offset)
            return cachedfile.read(size)

    def wait_data(self, offset, size):
        while 1:
            with self.condition:
                if not self.session.response:
                    logger.debug("HTTPResponse not ready, wait...")
                    self.condition.wait(0.01)
                    continue

                status_code = self.session.response.status_code
                # logger.debug("status_code=%d" % status_code)
                if status_code == -1:
                    self.condition.wait(0.01)
                    continue
                elif status_code != 200:
                    raise OpenAPIException("http error")

                cachefile = self.session.cachefile
                cachefilesize = len(cachefile)
                if offset + size < cachefilesize:
                    cachefile.seek(offset)
                    return cachefile.read(size)
                elif cachefilesize == self.filesize:
                    if offset < cachefilesize:
                        cachefile.seek(offset)
                        return cachefile.read(size)
                    else:
                        return ''
                else:
                    self.condition.wait(0.01)
            
    def end_download_file(self):
        with self.condition:
            logger.debug("file %s download completed" % (self.path, ))
            self.notify()

    def notify(self):
        with self.condition:
            self.condition.notify()

    def resultcallback(self, status):
        with self.condition:
            logger.debug(status)
            self.conidtion.notify()

    def run(self):
        logger.debug("start download %s" % self.path)
        self.session.prepare(self.downloadurl, callback=self.notify)
        try:
            self.api.download_file2(self.session)
            print len(self.session.response.cachefile)
        except OpenAPIError,apiex:
            logger.debug('download file2 error.')
            self.session.response.status_code = 500
            self.notify()

class WriteTask(object):

    def __init__(self, api, hashpath, path, filename, uploadqueue):
        self.api = api
        self.hashpath = hashpath
        self.path = path
        self.filename = filename
        self.fullpath = CACHE_PATH + hashpath
        self.writebytes = 0
        self.filesize = 0
        self.uploadqueue = uploadqueue
        self.clsname = "_" + self.__class__.__name__



    def end_download_file(self):
        with self.condition:
            logger.debug("file %s download completed" % (self.path, ))
            # os.unlink(self.tmpfilename)
            self.notify()

    def notify(self):
        with self.condition:
            self.condition.notify_all()

    def resultcallback(self, status):
        with self.condition:
            logger.debug(status)
            self.conidtion.notify()

    def run(self):
        logger.debug("start download %s" % self.path)
        self.tmpfile = open(self.tmpfilename, "w") 
        self.session = HTTPSession(tmpfile=self.tmpfile, callback=self.notify)
        self.response = self.api.download_file2(self.downloadurl, self.session)
        print len(self.response.text)
        self.tmpfile.close()

class WriteTask(object):

    def __init__(self, api, hashpath, path, filename, uploadqueue):
        self.api = api
        self.hashpath = hashpath
        self.path = path
        self.filename = filename
        self.fullpath = CACHE_PATH + hashpath
        self.writebytes = 0
        self.filesize = 0
        self.uploadqueue = uploadqueue
        self.clsname = "_" + self.__class__.__name__


    def start_upload_file(self, data, offset):
        if os.path.exists(self.fullpath):
            os.unlink(self.fullpath)

        with open(self.fullpath, "w") as f:
            f.seek(offset)
            f.write(data)


    def push_upload_file(self, data, offset):
        if os.path.exists(self.fullpath):
            with open(self.fullpath, "a") as f:
                f.seek(offset)
                f.write(data)


    def end_upload_file(self):
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
            self.__dropcache()
            if self.hashpath in self.uploadqueue:
                del self.uploadqueue[self.hashpath]

    def __dropcache(self):
        try:
            os.unlink(self.fullpath)
        except Exception, e:
            logger.error(e)


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
        self.uploadqueue = {}

    def initkuaipan(self):
        localtree = self.loadtree()
        logger.debug(localtree)

        if localtree: 
            self.fileprops = localtree
        else:
            logger.warn("no tree.db")

        ThreadPool.instance().start()
        threadpool = ThreadPool.instance()
        threadpool.delay(self.setaccountinfo, 0)
        threadpool.peroidic(self.setaccountinfo, 300)
        threadpool.delay(self.updatefileprops, 1)

        
    def __call__(self, op, *args):
        try:
            return super(KuaiPanFuse, self).__call__(op, *args)
        except Exception, e:
            if isinstance(e, OpenAPIError) or isinstance(e, OpenAPIException):
                raise FuseOSError(EIO)
            else:
                raise
            
    def init(self, path):
        logger.debug("fuse system already started!")
        self.initkuaipan()

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
            self.walked = True

    def chmod(self, path, mode):
        logger.debug("chmod %s" % path)

    def __listfiles(self, path):
        allfiles = ['.','..']
        for remotefile in filter(lambda node:node["st_parent_dir"] == path and node["st_name"] != '/', self.fileprops.itervalues()):
            allfiles.append(remotefile["st_name"])
        return allfiles

    def __hashpath(self, path, enablepid=False):
        uid, gid, pid = fuse_get_context()
        if enablepid:
            return sha1(path.encode("utf-8")).hexdigest().strip() + "_" + str(pid)
        else:
            return sha1(path.encode("utf-8")).hexdigest().strip()

    def readdir(self, path, fh):
        return self.__listfiles(path)

    def getattr(self, path, fh=None):
        ''' 'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid' '''
        if path in self.fileprops:
            return self.fileprops[path]
        else:
            raise FuseOSError(ENOENT)

    def read(self, path, size, offset, fh):
        uid, gid, pid = fuse_get_context()
        hashpath = self.__hashpath(path, enablepid=True)
        downloadtask = self.taskpool.query_download_task(hashpath)
        try:
            if downloadtask:
                logger.debug("task already existed! pid:%d" % pid)
                return downloadtask.wait_data(offset, size)
            else:
                filesize = self.fileprops[path]["st_size"]
                downloadtask = self.taskpool.download_file(hashpath, self.api, path, filesize)
                logger.debug("create download task")
            return downloadtask.wait_data(offset, size)
        except OpenAPIException, apiex:
            raise FuseOSError(EIO)

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
        hashpath = self.__hashpath(path)
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
        return 0 if path in self.fileprops else FuseOSError(EROFS)

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
        if os.path.basename(path) == ".Trash-1000":
            raise FuseOSError(ENOENT)

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
        hashpath = self.__hashpath(path)
        filename = os.path.basename(path)
        writetask = self.taskpool.query_upload_task1(hashpath)
        if writetask:
            writetask.sendmesg("push_upload_file", (data, offset))
            self.fileprops[path]["st_size"] += len(data)
        else:
            task = self.taskpool.upload_file(hashpath, self.api, path, filename, self.uploadqueue)
            task.sendmesg("start_upload_file", (data, offset))
            self.fileprops[path]["st_size"] = len(data)
        return len(data)    


    def flush(self, path, fh):
        logger.debug("flush file %s, pid is:%d" % (path, fuse_get_context()[2]))
        hashpath = self.__hashpath(path, enablepid=True)
        logger.debug(hashpath)
        downloadtask = self.taskpool.query_download_task(hashpath)
        if downloadtask:
            downloadtask.end_download_file()
            self.taskpool.delete_download_task(hashpath)

    def release(self, path, fh):
        '''called by fuse release a file'''
        hashpath = self.__hashpath(path)
        uploadtask = self.taskpool.query_upload_task1(hashpath)
        if uploadtask:
            uploadtask.sendmesg("end_upload_file", ())
            uploadtask.sendmesg("end_task", ())
            self.__waituploadready(path)
            self.taskpool.delete_upload_task(hashpath)

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
        with self.rlock:
            from pkg_resources import resource_filename
            filename = resource_filename("kuaipandriver", "tree.db")
            if os.path.exists(filename):
                from pickle import load
                return load(file(filename))

    def savetree(self):
        '''cache directory tree to local disk'''
        with self.rlock:
            from pkg_resources import resource_filename
            filename = resource_filename("kuaipandriver", "tree.db")
            if not self.fileprops:
                pass
            with open(filename, "w") as treefile:
                from pickle import dump
                dump(self.fileprops, treefile)
                logger.debug("dump ok" + str(self.fileprops))

    def destroy(self, path):
        '''called by fuse destory context'''
        logger.debug("fuse exited!")
        self.savetree()

def main():
    from kuaipandriver.common import getauthinfo, checkplatform
    checkplatform()
    from kuaipandriver.kuaipanapi import KuaipanAPI
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
                KuaiPanFuse(api), mntpoint, foreground=True, nothreads=False, 
                debug=False, big_writes=True, gid=os.getgid(), uid=os.getuid(), 
                umask='0133'
            )
    except RuntimeError as err:
        print str(err)
