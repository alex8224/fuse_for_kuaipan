Fuse For KuaiPan
===================

基于快盘的API实现的可以在linux下面使用的Fuse客户端，运行程序后，会生成一个linux下的挂载点，以普通文件夹的方式操作快盘。

目前实现了以下功能：

  1. 文件的浏览
  
  2. 文件异步上传， 下载
  
  3. 文件的管理， 建立目录，删除目录，创建文件，删除文件
  
  4. 显示快盘的总容量和使用容量，支持 df 命令
  
  5. 虚拟打印机， 上传office格式的文件到 `/convert/` 目录下会自动对上传的文档进行转换成网页格式并自动保存到指定的目录
  
  
使用要求
~~~~~~~~~

  1. 有快盘的账号
  
  2. 在快盘创建了至少一个应用， 需要用到里面的 `consumer_key` 和 `consumer_secret`, 可以到 http://www.kuaipan.cn/developers/create.htm 进行创建。

  3. 在创建好应用之后， 指定了自己的账号作为测试账号。
  
  
安装
~~~~~~

  pip install kuaipandriver


配置
~~~~~~

创建文件 config.json, 格式如下：

.. code-block:: javascript

  {
  "mntpoint":"挂载点的路径",
  "username": "登录快盘的账号",
  "password": "登录密码",
  "keylist": [
          ["consumer_key", "consumer_secret"],
          ...
      ]
  }


启动
~~~~~~

  mount.kuaipan -c config.json
  
  
