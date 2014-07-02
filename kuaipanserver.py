# -*-:coding:utf-8 -*-

import gevent
from gevent import monkey
monkey.patch_all()

import os
from Queue import Queue
from hashlib import sha1
from threading import Thread
from SimpleXMLRPCServer import SimpleXMLRPCServer

TMP_PATH = "/tmp/"

class WriteRequest(object):
    def __init__(self, hashpath):
        pass

class WriteRequest(Thread):
    def __init__(self, hashpath):
        self.writequeu = Queue(1000)
        self.hashpath = hashpath

    def push_to_request(self):
        pass

    def run(self):
        pass

    def new_write_request(self, hashpath, data, offset):
        gevent.spawn(self._new_write_request, hashpath, data, offset)

    def _new_write_request(self, hashpath, data, offset):
        pass

    def query_write_quest(self, hashpath):
        return self.task.get(hashpath, None)

# rpc接口提供方法
class TaskRPC(object):

    def query_upload_request(hashpath):
        pass

    def upload_request(hashpath, data, offset):
        pass


    def commit_upload_request(hashpath):
        '''返回写完的缓存文件路径'''
        pass



class WriteRequest(object):

    def __init__(self, path):
        self.path = path
        self.hashpath = sha1(path).hexdigest().strip()
        self.filelist = {}

    def query_request(self, path):
        pass

    def add_new_request(self):
        '''启动写文件任务, 通过首次初始化的文件'''
        pass

    def commit_upload_request(self, hashpath):
        '''通知完成写入, 是在这里写还是在调用端写'''
        pass


if __name__ == '__main__':
    server = MessageServer("/tmp/mess.sock", MessageHandler)
    server.serve_forever()
