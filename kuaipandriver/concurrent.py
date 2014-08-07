# -*-coding:utf-8 -*-
#********************************************************************************
#Author: alex8224@gmail.com
#Create by: 2013-08-17 12:53
#Last modified: 2014-07-01
#Filename: concurrent.py
#Description: 
# 是一个线程池，用来执行异步/并发的任务
# 能够指定 max_thread_num, free_thread_num 和　idle_seconds
# 自动维持线程池中线程的数量，自动清理超过idle时间的线程
# 支持 异步任务 ,使用 Future 模式
#********************************************************************************

import time
from Queue import Queue
from collections import deque
from types import FunctionType, MethodType
from threading import Thread, Condition, RLock as Lock
from kuaipandriver.common import Singleton, config, logger

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

class Callable(Future):
    def __init__(self, callable_):
        super(Callable, self).__init__()
        self.callable_ = callable_

    def __call__(self, *args, **kwargs):
        return self.callable_(*args, **kwargs)

class ThreadPool(Singleton, Thread):

    def __init__(self):
        Thread.__init__(self)
        self.max_thread_num = int(config.get_max_thread_num())
        self.free_thread_num = int(config.get_free_thread_num())
        self.idle_seconds = int(config.get_idle_seconds())
        self.runflag = False
        self.freequeue = deque()
        self.busyqueue = deque()
        self.delayqueue = deque()
        self.lock = Lock()
        self.condition = Condition(self.lock)
        self.setDaemon(True)
        self.initpool()

    def initpool(self):
        for workerindex in range(self.free_thread_num):
            self.__createworker()

    def quit(self):

        '''wait for all busy task done'''''
        while 1:
            with self.condition:
                if len(self.busyqueue) > 0:
                    self.condition.wait(1)
                else:
                    break

        for task in self.freequeue:
            task.quit()
            task.waitdone()
            
        with self.lock:
            self.runflag = False    
        
        self.join()

    def __createworker(self):
        worker = Worker()
        self.freequeue.append(worker)
        return worker

    def __dotaskinworker(self, callable_, *args, **kwargs):
        worker = self.freequeue.popleft()
        self.busyqueue.append(worker)
        if type(callable_) in (FunctionType, MethodType):
            callable_ = Callable(callable_)

        worker.execute(callable_, *args, **kwargs)
        if callable(callable_):
            return callable_
        else:
            return worker

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
                self.condition.wait()
                return self.__dotaskinworker(callable_, *args, **kwargs)

    def taskdone(self, worker):
        with self.condition:
            self.condition.notify()
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

            
            time.sleep(0.5)

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
            logger.error(taskex)
            task.set_result(taskex)
            self.status = "FREE"
        finally:
           ThreadPool.instance().taskdone(self)

    def run(self):
        while 1:
            cmd, task, args, kwargs = self.taskqueue.get()
            if cmd == "QUIT":
                self.status = "QUIT"
                self.lastupdate = time.time()
                with self.waitobj:
                    self.waitobj.notify()
                break
            else:
                if callable(task):
                    self.runascallable(task, *args, **kwargs)
                else:
                    self.runasinteractive(task)
