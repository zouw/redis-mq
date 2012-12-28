# coding: utf-8
import socket,conf,time
from os.path import basename, join
#from zkclient import ZKClient, zookeeper, watchmethod
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

import constant,json

def subscriber_watcher(handler, type, state, path):
    conf.logger.info("func=zkWrapper handler:"+str(handler)+",type:"+str(type)+",state:"+str(state)+",path:"+path)
#    zookeeper.get(handler,path,myWatch);

class zkWrapper:
    def __init__(self, cfg):
        self.mq_path = cfg["config"]["zookeeper"][2]
        self.cfg = cfg
        self.load()

    def load(self):
        (self.enable, self.zkaddr, self.zkpath) = self.cfg["config"]["zookeeper"]
        idx = 0
        while True:
            idx = (idx % len(self.zkaddr))
            try:
                if self.enable:
                    #self.zk = ZKClient(self.zkaddr[idx], constant.ZK_TIME_OUT)
                    self.zk = KazooClient(self.zkaddr[idx], constant.ZK_TIME_OUT)
                    self.zk.start()
                conf.logger.debug("func=zkWrapper:load success")
                break
            except Exception,x:
                conf.logger.error("func=zkWrapper:load failed desc:%s" % (str(x)))
                time.sleep(2)
                continue

    def get_children(self, path, watcher):
        addr = self.zkpath + "/" + path
        return self.zk.get_children(addr, watcher)

    def get_data(self, path, watcher):
        addr = self.zkpath + "/" + path
        return self.zk.get_data(addr, watcher)

    def register(self, host, port, path):
        """
        register a node for this worker
        """

        addr = self.mq_path + "/" + path + "/" + host + ":" + str(port)

        while True:
            try:
                self.zk.ensure_path(addr, None)
                conf.logger.info("register success. %s" % (addr))
                break
            except NodeExistsError, x:
                conf.logger.error("register succuss(%s). addr(%s))" % (str(x),addr))
                break
            except Exception,x:
                conf.logger.error("register failed. addr(%s) desc(%s)" % (addr, str(x)))
                time.sleep(0.5)
                continue

if __name__ == "__main__":
    #import utils
    f = file(constant.MQ_CONF_PATH)
    c = json.load(f)
    f.close()
    zk = zkWrapper(c)
    #host = utils.g_var.get_host_ip_port()
    #zk.register(host[0], host[1], ".service_nodes")
    zk.register("127.0.0.1", "2181", ".service_nodes")
    time.sleep(4)
