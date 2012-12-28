#!/usr/bin/env python
# author:zouwan@gmail.com
# create time : 2012.12.12
# desc:
#  zookeeper wrapper
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
import socket,conf,time,traceback
from os.path import basename, join
from zkclient import ZKClient, zookeeper, watchmethod
import constant,json

class zkWrapper:
    def __init__(self, util):
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)
        self.cfg = util.cfg
        self.zk  = None
        self.load()

    def load(self):
        (self.enable, self.zkaddr, self.zkpath) = self.cfg["config"]["zookeeper"]
        try:
            if self.enable:
                self.zk = ZKClient(self.zkaddr[0], constant.ZK_TIME_OUT)
                conf.logger.info("zk client create success")
        except Exception,x:
            conf.logger.error("zk client create failed. desc:%s" % (traceback.format_exc()))

    def get_children(self, path, watcher):
        """
        get children from zookeeper
        @path relative path of root mq path of zk(/3g/ice/x2/mq
        @watcher  watch zk
        """
        (addr,addrlist) = ("",[])
        try:
            addr = self.zkpath + "/" + path
            conf.logger.info("get_children %s" % (addr))
            addrlist = self.zk.get_children(addr, watcher)
        except zookeeper.NodeExistsException, x:
            conf.logger.warn("func=zkwrapper:get_children addr=%s desc=%s." % (addr, str(x)))
        except Exception,x:
            conf.logger.error("func=zkwrapper:get_children addr=%s desc=%s. recreate zk client" % (addr, str(x)))
            self.zk.close()
            self.zk = ZKClient(self.zkaddr[0], constant.ZK_TIME_OUT)
            time.sleep(2)
        finally:
            return [] if addrlist==None else addrlist

    def get_data(self, path, watcher):
        """
        get data from zookeeper
        @path relative path of root mq zk path(/3g/ice/x2/mq)
        @watcher  watch zk
        """
        (addr,data) = ("","")
        try:
            addr   = self.zkpath + "/" + path
            return self.zk.get(addr, watcher)
        except Exception,x:
            conf.logger.error("func=zkwrapper:get_data addr=%s desc=%s. recreate zk client" % (addr, str(x)))
            self.zk.close()
            self.zk = ZKClient(self.zkaddr[0], constant.ZK_TIME_OUT)
            time.sleep(2)


    def register(self, ip, port):
        """
        register a node for this worker
        @ip
        @port
        @path relative path of root mq zk path(/3g/ice/x2/mq)
        """
        @watchmethod
        def watcher(event):
            conf.logger.info("func=register event=%s " % (str(event.path)))
            self.register(ip,port)

        rpath = "/.service_nodes/" + ip + ":" + str(port)
        addr  = self.zkpath + rpath

        while True:
            try:
                path = self.zk.create(addr, "", flags=zookeeper.EPHEMERAL)
                conf.logger.info("register success. %s" % (addr))
                break
            except zookeeper.ConnectionLossException,x:
                self.zk = ZKClient(self.zkaddr[0], constant.ZK_TIME_OUT)
                conf.logger.info("zk client create success")
            except zookeeper.NodeExistsException, x:
                conf.logger.info("register succuss(%s). addr(%s))" % (addr, x))
                break
            except Exception,x:
                conf.logger.error("register failed. addr(%s) desc(%s)" % (addr, traceback.format_exc()))
                time.sleep(constant.SLEEP_ZK_REGISTER)
                continue
        try:
            self.zk.exists(addr, watcher)
        except Exception,x:
            conf.logger.error("mq register node not exist. desc:%s" %(traceback.format_exc()))

if __name__ == "__main__":
    import json,constant,utils
    util = utils.GlobalVar()
    f = file(constant.MQ_CONF_PATH)
    c = json.load(f)
    f.close()
    zk = zkWrapper(c)
    host = util.get_host_ip_port()
    zk.register(host[0], host[1], ".service_nodes")
    import time
    time.sleep(1000)
