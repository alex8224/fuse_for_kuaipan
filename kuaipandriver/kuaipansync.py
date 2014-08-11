#!/usr/bin/python
# -*-coding:utf-8 -*-
#****************************************************
# Author: alex8224@gmail.com
# Create by: 2013-08-17 11:01
# Last modified: 2014-07-08 11:45
# Filename: kuaipansync.py
# Description:
# monitor local dir and sync created/modifyed/ file to kuaipandriver
# ToDo:
# 1. bidirection sync?
#

import os
import sys
import pyinotify
from shutil import copyfile
from Queue import Queue
from threading import Thread
from pyinotify import IN_CREATE, IN_DELETE, IN_ACCESS, IN_CLOSE_NOWRITE, \
        IN_CLOSE_WRITE, IN_MODIFY, IN_MOVED_FROM, IN_MOVED_TO, IN_OPEN 

wm = pyinotify.WatchManager()  # Watch Manager
mask = IN_CREATE|IN_DELETE|IN_ACCESS|IN_CLOSE_NOWRITE|IN_CLOSE_WRITE|IN_MODIFY|IN_MOVED_FROM|IN_MOVED_TO|IN_OPEN 
watchdir = syncdir = None

def filternames(func):
    def _intern(*args, **kwargs):
        fullpath = os.path.basename(args[0]).lower()
        if (not fullpath.startswith(".")) and fullpath.find(".") > -1 and (not fullpath.endswith(".tmp")):
            return func(*args, **kwargs)
    return _intern

class SyncTask(Thread):
    def __init__(self):
        super(SyncTask, self).__init__()
        self.task_queue = Queue(1000)
        self.setDaemon(True)

    def put_task(self, callable_, *args):
        self.task_queue.put_nowait((callable_, args))

    def run(self):
        while 1:
            callablefunc, args = self.task_queue.get()
            callablefunc(*args)
@filternames
def removetask(src):
    destfile = syncdir + os.sep + src[len(watchdir):]
    if os.path.exists(destfile):
        os.unlink(destfile)
    print("delete file %s, destfile %s" % (src, destfile))

@filternames
def createtask(src):
    destfile = syncdir + os.sep + src[len(watchdir):]
    if os.path.exists(destfile):
        os.unlink(destfile)
    
    dirname = os.path.dirname(destfile)
    if not os.path.exists(dirname):
        os.system("mkdir -p %s" % dirname)    
    copyfile(src, destfile)
    print("%s create ok, source file %s" % (destfile, src))

class EventHandler(pyinotify.ProcessEvent):
    # def process_IN_CREATE(self, event):
        # print "Creating:", event.pathname
    
    def set_background_thread(self, threadobj):
        self.bgthread = threadobj

    def process_IN_DELETE(self, event):
        self.bgthread.put_task(removetask, event.pathname)

    # def process_IN_ACCESS(self, event):
        # print "access", event.pathname

    # def process_IN_CLOSE_NOWRITE(self, event):
        # print "closenowrite", event.pathname

    def process_IN_CLOSE_WRITE(self, event):
        self.bgthread.put_task(createtask, event.pathname)

    # def process_IN_MODIFY(self, event):
        # print "IN_MODIFY", event.pathname

    def process_IN_MOVED_FROM(self, event):
        print "move from", event.pathname

    def process_IN_MOVED_TO(self, event):
        self.bgthread.put_task(createtask, event.pathname)

    # def process_IN_OPEN(self, event):
        # print "OPEN FILE", event.pathname

def help():
    print("<scriptname> watchdir syncdir")
    sys.exit(1)

def main():
    if len(sys.argv) != 3:
        help()

    global watchdir
    global syncdir
    watchdir, syncdir = sys.argv[1], sys.argv[2]

    synctask = SyncTask()
    synctask.start()
    handler = EventHandler()
    handler.set_background_thread(synctask)
    notifier = pyinotify.Notifier(wm, handler)
    wdd = wm.add_watch(watchdir, mask, rec=True)
    notifier.loop()
