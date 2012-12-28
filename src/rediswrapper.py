import conf,redis,time,constant,utils,MessageProxy
from zkclient import ZKClient, zookeeper, watchmethod

class RedisEx:
        """
        redis wrapper
        wrap every op of redis
        1. if config use zk, read redis addr from zk and watch redis node
        2. otherwise, read redis addr from config file
        """
        def __init__(self, util):
            """
            conn
            {
             '1': [0, [[['127.0.0.1', 36667], <redis.client.Redis object at 0x2931450>],
                       [['127.0.0.1', 36666], <redis.client.Redis object at 0x29314d0>]]],
             '2': [0, [[['127.0.0.1', 36667], <redis.client.Redis object at 0x2931450>],
                       [['127.0.0.1', 36666], <redis.client.Redis object at 0x29314d0>]]]
            }
            """
            #        self.addr  = dict()
            self.util = util
            self.conn  = dict()
            self.retry = dict()       # used for retry connect redis server count stat
            self.load()

        def load(self):
            if self.util.cfg["config"]["zookeeper"][0] == "true":
                self.parse_zk()
            else:
                self.parse_lc()

        def parse_lc(self):
            for k in self.util.cfg["config"]["redis"].keys():
                if not self.conn.has_key(k):
                    self.conn[k]=[0, []]
                for j in range(0, len(self.cfg["config"]["redis"][k])):
                    if j >= len(self.conn[k][1]):
                        self.conn[k][1].append([self.cfg["config"]["redis"][k][j],None])
                    self.retry_connect_rdx_idx(k,j)

        def parse_zk(self):
            """
            get redis addrs from zk and update.
            """
            @watchmethod
            def watcher(event):
                conf.logger.info("func=RedisEx:parse_zk:watcher event=%s" % (str(event)))
                self.parse_zk()

            # get sharding info from zk
            (addrlist,shardings,addr) = ([],[],"redis")
            self.shardings = self.util.zk.get_children(addr, watcher)
            if len(self.shardings)==0:
                conf.logger.error("no redis sharding info from zk")
                return

            for rdx in self.shardings:
                self.conn[rdx]=[0, []]
                addr = "redis/" + str(rdx) + "/" + constant.ZK_SUFFIX_PATH
                addrlist = self.util.zk.get_children(addr, watcher)
                conf.logger.debug("get redis sharding="+rdx + " list:"+str(addrlist))
                l = len(addrlist) - len(self.conn[rdx][1])
                while l>0:
                    self.conn[rdx][1].append([])
                    l -= 1
                for idx in range(0, len(addrlist)):
                    sp = addrlist[idx].split(":")
                    addr=[sp[0], int(sp[1])]
                    self.conn[rdx][1][idx]=[addr, None]
                    self.retry_connect_rdx_idx(rdx, idx)
            #notify dispatcher or directly add/remove process
            self.util.update_disp = 1

        #return one success connection with redis
        def retry_connect_rdx(self, rdx):
            for idx in range(0, len(self.conn[rdx][1])):
                if self.retry_connect_rdx_idx(rdx,idx):
                    self.conn[rdx][0] = idx
                    conf.logger.debug("func=rediswrapper:retry_connect_rdx idx=%d" % (idx))

        #return one success connection with redis
        def retry_connect_rdx_idx(self, rdx, idx):
            """
            self.conn must have key rdx, and idx is valid
            """
            if not self.conn.has_key(rdx):
                conf.logger.warn("func=rediswrapper:retry_connect_rdx_idx , rdx(%d) not find" % (idx))
                return None
            if len(self.conn[rdx][1])<=idx:
                conf.logger.warn("func=rediswrapper:retry_connect_rdx_idx , idx(%d) out range" % (idx))
                return None

            addr = self.conn[rdx][1][idx][0]
            try:
                conn = redis.Redis(addr[0], addr[1])
                self.conn[rdx][1][idx][1] = conn
                conf.logger.info('fun=redisWrapper:retry_connect_rdx_idx success' +
                                 'rdx:idx=%s:%d redis(%s:%d)' %
                                  (rdx,idx,addr[0],addr[1]))
            except Exception, x:
                self.conn[rdx][1][idx][1] = None
                conf.logger.warn('fun=redisWrapper:retry_connect_rdx_idx failed' +
                                 'rdx:idx=%s:%d redis(%s:%d). desc: %s' %
                                  (rdx,idx,addr[0],addr[1], x))


if __name__ == "__main__":
    time.sleep(10000)
