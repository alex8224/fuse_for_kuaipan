# -*-coding:utf-8 -*-
#********************************************************************************
#Author: alex8224@gmail.com
#Create by: 2013-08-17 12:53
#Last modified: 2014-07-01
#Filename: kuanpanfuse.py
#Description: 
# 实现快盘的ＡＰＩ,可以实现文件的异步上传, 下载，浏览，目录, 文档转换操作
# 支持配置本地读缓存，支持异步写
# 支持查看网盘容量
# 支持文档转换，以虚拟打印机的方式实现
# 缩略图和版本功能未实现
# todo
# README.MD on github
# 3. test host speed and using fastest connection download file
#********************************************************************************

import os
import sys
import time
import signal
import pycurl
from copy import copy
from hashlib import sha1
from httputils import HTTPSession
from functools import partial
from stat import S_IFDIR, S_IFREG
from errno import ENOENT, EROFS, EEXIST, EIO
from threading import Thread, Condition, RLock as Lock
from kuaipandriver.concurrent import Future, ThreadPool
from kuaipandriver.kuaipanapi import KuaipanAPI, OpenAPIError, OpenAPIException
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
from kuaipandriver.common import timestamp, to_timestamp, define, SafeLRUCache, \
        DiskCacheable, gethomedir, config, logger, CopyOnWriteBuffer

TYPE_DIR = (S_IFDIR | 0644 )
TYPE_FILE = (S_IFREG | 0644 )
BLOCK_SIZE = 4096

CACHE_PATH = "/dev/shm/"

ROOT_ST_INFO = {
        "st_mtime": timestamp(),
        "st_mode":  TYPE_DIR,
        "st_size":  BLOCK_SIZE,
        "st_gid":   os.getgid(),
        "st_uid":   os.getuid(),
        "st_atime": timestamp(),
        "st_nlink": 2,
        "st_parent_dir":".",
        "st_name": "/"
        }



def handler(signum, frame):
    print "use press ctrl+c exit"
    sys.exit(0)

signal.signal(signal.SIGINT, handler)

def unique_id():
    from hashlib import md5
    from uuid import uuid1
    return md5(str(uuid1())).hexdigest()


class TaskPool(object):
    def __init__(self):
        self.uploadpool = {}
        self.downloadpool = {}
        self.uploadlock, self.downlock = Lock(), Lock()
        self.taskclass = {"upload":WriteTask, "download":DownloadTask}

    def upload_file(self, hashpath, *args, **kwargs):
        params = list(args)
        params.insert(1, hashpath)
        task = ThreadPool.instance().submit(WriteTask(*params, **kwargs))
        self.uploadpool[hashpath] = task
        return task

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
                        "st_mtime": to_timestamp(finfo["modify_time"]),
                        "st_mode":  TYPE_FILE if finfo["type"] == "file" else TYPE_DIR,
                        "st_size":  int(finfo["size"]) if finfo["type"] == "file" else BLOCK_SIZE,
                        "st_gid":   os.getgid(),
                        "st_uid":   os.getuid(),
                        "st_atime": timestamp(),
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

        self.session.close()            
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
        hashpath = sha1(path.encode("utf-8")).hexdigest().strip()
        SafeLRUCache.instance().remove(hashpath)
        return self.rmdir(path)

    def rename(self, oldpath, newpath):
        return self.api.move(oldpath, newpath)

    def accountinfo(self):
        return self.api.account_info()


class DownloadTask(Thread):

    def __init__(self, api, hashpath, path, filesize, notify):
        Thread.__init__(self)
        self.api = api
        self.hashpath = hashpath
        self.cachekey = hashpath[0:hashpath.find("_")]
        self.path = path
        self.filesize = filesize
        self.condition = Condition()
        self.cache = SafeLRUCache.instance()
        self.cachefile = CopyOnWriteBuffer()
        self.fromcache = False
        self.cachevalue = self.querycache()
        self.success = True

    def querycache(self):
        cachefile = self.cache.get(self.cachekey)
        if cachefile:
            try:
                value = cachefile.value
                self.fromcache = True
                return value
            except IOError, ioe:
                logger.error(ioe)
                self.cache.remove(self.cachekey)

    def savetocache(self, data):
        diskcachefile = DiskCacheable()
        diskcachefile.key = self.cachekey
        diskcachefile.value = data
        self.cache.set(self.cachekey, diskcachefile)
        logger.debug("save %s to cachefile" % self.hashpath)

    def readfromcache(self, offset, size):
        with self.condition:
            cachefile = self.cachevalue
            if cachefile:
                if offset > self.filesize:
                    return ''
                cachefile.seek(offset)
                return cachefile.read(size)

    def readfromserver(self, offset, size):

        while 1:
            with self.condition:
                if not self.success:
                    return ''
                cachefilesize = self.cachefile.length
                if offset + size < cachefilesize:
                    self.cachefile.seek(offset)
                    return self.cachefile.read(size)
                elif cachefilesize == self.filesize:
                    if offset < cachefilesize:
                        self.cachefile.seek(offset)
                        return self.cachefile.read(size)
                    else:
                        #why offfset > length?
                        return ''
                else:
                    self.condition.wait(1)

    def wait_data(self, offset, size):
        if self.fromcache:
            return self.readfromcache(offset, size)
        else:
            return self.readfromserver(offset, size)

    def end_download_file(self):
        with self.condition:
            logger.debug("file %s download completed" % self.path)
            if not self.fromcache and self.success:
                self.cachefile.seek(0)
                self.savetocache(self.cachefile.read())
            self.notify()

    def notify(self):
        with self.condition:
            self.condition.notify()

    def run(self):
        if not self.fromcache:
            logger.debug("start download %s" % self.path)
            session = HTTPSession()
            try:
                urlresult = self.api.get_downloadurl(self.path, session)
                if urlresult.status_code != 302:
                    raise OpenAPIException("get_download_url failed %s" % urlresult.text)
                downloadurl = urlresult.headers["location"]
                self.api.download_file2(downloadurl, self.notify, self.cachefile, session)
            except (OpenAPIError, OpenAPIException) as openex:
                logger.debug(openex)
                self.success = False
                self.notify()
            except pycurl.error as pyerror:
                logger.debug("download file error, reason is %s" % pyerror[1])
                self.success = False
                self.notify()
            finally:
                session.close()
        else:
            logger.debug("download file from cache")


class WriteTask(object):

    def __init__(self, api, hashpath, path, filename, uploadqueue, handler=None):
        self.api = api
        self.hashpath = hashpath
        self.path = path
        self.filename = filename
        self.fullpath = CACHE_PATH + hashpath
        self.uploadqueue = uploadqueue
        self.clsname = "_" + self.__class__.__name__
        self.afteruploadhandler = handler 


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
                    self.afteruploadhandler(self.api)
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


class AfterUploadHandler(object):
    def __init__(self, api, path, fuse):
        self.api = api
        self.path = path
        self.outputdir = config.get_file_convert_out_dir()
        self.virtualdir = config.get_virtual_convert_dir()
        self.threadpool = ThreadPool.instance()
        self.fuse = fuse

    def async_convert(self, api, viewtype):
        if not os.path.exists(self.outputdir):
            os.mkdir(self.outputdir)

        fullpath = os.path.join(self.outputdir, os.path.basename(self.path)) + ".zip"
        result = api.convert(self.path, viewtype)
        if result.status_code == 200:
            with open(fullpath, "w") as zipfile:
                zipfile.write(result.raw.read())

            logger.debug("%s convert ok" % fullpath)
            self.fuse.unlink(self.path)
        else:
            logger.debug("convert document failed, the reason is %s" % result.text)

    def __call__(self, *args):
        dirname = os.path.dirname(self.path)
        if dirname != self.virtualdir:
            return
        logger.debug("converting %s to html format to %s" % (self.path, self.outputdir))
        _, extname = os.path.splitext(self.path)
        viewtype = "normal"
        if extname in ('.pdf', '.doc', '.wps', '.csv', '.prn', '.xls', '.et', '.ppt', '.dps', '.txt', '.rtf'):
            self.threadpool.submit(self.async_convert, self.api, viewtype)
        else:
            logger.debug("not support format: %s" % extname)

class KuaiPanFuse(LoggingMixIn, Operations):

    def __init__(self):
        self.api = KuaipanAPI.instance()
        self.root = "."
        self.fd = 0
        self.fileprops = {"/":ROOT_ST_INFO}
        self.taskpool = TaskPool()
        self.rootfiles = []
        self.cwd = ''
        self.rlock = Lock()
        self.quota = {"f_blocks":0, "f_bavail":0, "totalspace":0, "usedspace":0}
        self.walked = False
        self.uploadqueue = {}

    def initkuaipan(self):
        localtree = self.loadtree()

        if localtree: 
            self.fileprops = localtree
        else:
            logger.warn("no tree.db")
        self.mkdir(config.get_virtual_convert_dir(), 0644)
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
        self.log.debug("fuse system already started!")
        self.initkuaipan()
        self.initcache()

    def initcache(self):
        SafeLRUCache.instance().load()

    def setaccountinfo(self):

        def updatediskquota(result):
            try:
                _, quota = result
                totalspace, usedspace = quota["quota_total"], quota["quota_used"]
                availspace = totalspace - usedspace
                self.quota["f_blocks"] = totalspace / BLOCK_SIZE
                self.quota["f_bavail"] = availspace / BLOCK_SIZE
            except:
               import traceback;traceback.print_exc()

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
                return downloadtask.wait_data(offset, size)
            else:
                filesize = self.fileprops[path]["st_size"]
                downloadtask = self.taskpool.download_file(hashpath, self.api, path, filesize)
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
                fileinfo = copy(ROOT_ST_INFO)
                fileinfo["st_mode"] = TYPE_FILE
                fileinfo["st_nlink"] = 1
                fileinfo["st_parent_dir"] = os.path.dirname(path)
                fileinfo["st_name"] = os.path.basename(path)
                if size:
                    fileinfo["st_size"] = size
                self.fileprops[path] = fileinfo
            else:
                dirinfo = copy(ROOT_ST_INFO)
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
        writetask = self.taskpool.query_upload_task(hashpath)
        if writetask:
            writetask.sendmesg("push_upload_file", (data, offset))
            self.fileprops[path]["st_size"] += len(data)
        else:
            task = self.taskpool.upload_file(
                        hashpath, 
                        self.api, 
                        path, 
                        filename, 
                        self.uploadqueue,
                        handler=AfterUploadHandler(self.api, path, self)
                    )
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
        uploadtask = self.taskpool.query_upload_task(hashpath)
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
        filename = gethomedir() + os.sep + "tree.db"
        if os.path.exists(filename):
            from pickle import load
            return load(file(filename))

    def savetree(self):
        '''cache directory tree to local disk'''
        if not self.fileprops:
            return

        filename = gethomedir() + os.sep + "tree.db"
        with open(filename, "w") as treefile:
            from pickle import dump
            dump(self.fileprops, treefile)

    def destroy(self, path):
        '''called by fuse destory context'''
        self.savetree()
        SafeLRUCache.instance().save()
        logger.debug("fuse exited!")

def parse_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-c", "--configfile", dest="configfile", help="special config file fullpath")
    parser.add_option("-f", "--foreground", dest="foreground", default=False, action="store_true", help="do not fork into background.")

    (options, args) = parser.parse_args()

    if not options.configfile:
        parser.print_help()
    else:
        return options

def parse_config(configfile):
    if not os.path.exists(configfile):
        print("configfile %s not existed!" % configfile)
        sys.exit(1)
    from json import load
    try:
        cfg = load(file(configfile))
        define(mntpoint=cfg["mntpoint"], user=cfg["username"], pwd=cfg["password"], keylist=cfg["keylist"])
    except ValueError:
        print("config file format error")
        sys.exit(1)
        
def main():
    from common import checkplatform, Context
    checkplatform()
    options = parse_options()
    if not options:
        return

    parse_config(options.configfile)

    try:
        mntpoint = Context.instance().mntpoint
        FUSE(
                KuaiPanFuse(), mntpoint, foreground=options.foreground, nothreads=False, 
                debug=False, big_writes=True, gid=os.getgid(), uid=os.getuid(), 
                umask='0133'
            )
    except RuntimeError as err:
        print str(err)
