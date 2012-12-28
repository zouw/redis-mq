import socket,sys,json,time,MessageProxy
import redis,conf,constant
from zkwrapper import zkWrapper
from messagewrapper import messageWrapper
from rediswrapper import RedisEx
from multiprocessing import Value

sys.path.append("../dep/ice/")
import Ice
Ice.loadSlice("../slice/mq.ice")
import com.renren.x2.mq
mq = com.renren.x2.mq

class GlobalVar:
        def __init__(self):
            self.update_disp       = 1
            self.parse_config()

        def parse_config(self):
            self.process           = dict()
            self.cfg               = self.init_config()
            self.zk                = zkWrapper(self)
            self.msginfo           = messageWrapper(self)
            self.redis             = RedisEx(self)
            (self.host, self.port) = self.get_host_ip_port()

        def init_config(self):
            f                      = file(constant.MQ_CONF_PATH)
            cfg                    = json.load(f)
            f.close()
            return cfg

        def retry_connect_subscriber_idx(self, key, idx):
            """
            connect one subscriber
            """
            conf.logger.debug("old:%s " % (str(self.msginfo.msgmap[key][1][idx])))
            try:
                straddr = "MqService:tcp -h %s -p %d -t %d" % (self.msginfo.msgmap[key][1][idx][0], self.msginfo.msgmap[key][1][idx][1], constant.DISP_TIME_OUT)
                if self.msginfo.msgmap[key][1][idx][2]:
                    self.msginfo.msgmap[key][1][idx][2].destroy()
                conf.logger.debug("ice initialize  %s" % (straddr))
                self.msginfo.msgmap[key][1][idx][2] = Ice.initialize(sys.argv)
                base = self.msginfo.msgmap[key][1][idx][2].stringToProxy(straddr)
                conn = mq.MqServicePrx.checkedCast(base)
                self.msginfo.msgmap[key][1][idx][3] = conn
                conf.logger.info("success connect subscriber(%s:%d:%s) %s" %
                                 (self.msginfo.msgmap[key][1][idx][0], self.msginfo.msgmap[key][1][idx][1],
                                  conn, str(self.msginfo.msgmap[key][1])))
            except Exception, x:
                conf.logger.warn("failed connect subscriber(%s:%d) " %
                                 (self.msginfo.msgmap[key][1][idx][0], self.msginfo.msgmap[key][1][idx][1]))

        def retry_connect_subscriber(self, key):
            """
            connect all subscriber
            """
            if not self.msginfo.msgmap.has_key(key):
                return
            for idx in range(0, len(self.msginfo.msgmap[key][1])):
                self.retry_connect_subscriber_idx(key,idx)

        def get_host_ip_port(self):
            myname = socket.getfqdn(socket.gethostname())
            myaddr = socket.gethostbyname(myname)
            port   = constant.MQ_ICE_PORT
            return (myaddr, port)

        def check_port(self, host, port):
            try:
                sockobj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sockobj.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sockobj.connect((host, port))
                sockobj.close()
                conf.logger.error("check address(%s:%d) failed. address already used"
                                  % (host, port))
                return 1
            except Exception,x:
                conf.logger.info("check address(%s:%d) success"
                                  % (host, port))
                return 0

g_state = 0

def signal_pause(n,e):
    global g_state
    g_state = 1
    conf.logger.info("myhandler_pause")

def signal_restore(n,e):
    global g_state
    g_state = 0
    conf.logger.info("myhandler_restore")

if __name__ == "__main__":
    print "success"
