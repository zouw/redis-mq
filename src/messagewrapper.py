import utils,conf,time,constant,traceback
from zkclient import ZKClient, zookeeper, watchmethod

class messageWrapper:
    def __init__(self, util):
        """
        msgmap[queue]=[[1003,2001],     # subscriber's message type list
                      [['ip',port,ic,conn,try_count],['ip',port,ic,conn,try_count]],  # subscriber's ice info
                      redis_sharding_idx]  # subscriber's sharding in redis
        """
        self.util        = util
        self.msg2key     = dict()         # msgid --> key(queue name)
        self.msgmap      = dict()         # message mapping
        self.rcv_msg_num = 0              # recieved message num from server start
        self.rec_msg_num = 0              # recorded message num from server start
        self.parse_config()

    def parse_config(self):
        if self.util.cfg["config"]["zookeeper"][0] == "true":
            self.parse_zk()
        else:
            self.parse_lc()

    def parse_zk(self):
        """
        get redis addrs from zk and update.
        """
        @watchmethod
        def watcher(event):
            conf.logger.info("func=messageWrapper:parse_zk:watcher event=%s" % (str(event)))
            self.parse_zk()

        # get queue list in zk like /3g/ice/x2/MqService/msgmap/${queue_name}
        (queue_list, addr_list, msg2key, msgmap) = ([], [], dict(), dict())
        queue_list = self.util.zk.get_children("msgmap", watcher)
        if len(queue_list) == 0:
            conf.logger.info("func=messagewrapper:parse_zk msgmap list is 0.")
            time.sleep(2)
            return

        for i in queue_list:
            try:
                spath = "msgmap/"+i+"/"+constant.ZK_SUFFIX_PATH
                addr_list = self.util.zk.get_children(spath,watcher)
                conf.logger.info("func=messagewrapper:parse_zk %s have %d subscriber data=%s" % ("msgmap/"+i,len(addr_list),str(addr_list)))
            except:
                continue
            (sub_addr,sp,sub_msg)  = ([],[],[])
            for j in addr_list:
                sp = j.split(":")
                if len(sp) == 2:
                    sub_addr.append([sp[0],int(sp[1]),None,None,0])
            try:
                sub_msg_l = self.util.zk.get_data("msgmap/"+i, watcher)
                conf.logger.debug("func=messagewrapper:parse_zk %s" % (str(sub_msg_l)))
                if len(sub_msg_l[0])<1:
                    continue
            
                sub_msg   = eval(sub_msg_l[0])
                if len(sub_msg)==0:
                    continue
                for m in sub_msg[1]:
                    if not msg2key.has_key(m):
                        msg2key[m] = set()
                    msg2key[m].add(i)
                msgmap[i] = [sub_msg[1], sub_addr, sub_msg[0]]
            except Exception,x:
                conf.logger.error("func=messagewrapper:parse_zk %s" % (traceback.format_exc()))
        self.msg2key = msg2key
        self.msgmap  = msgmap
        self.util.update_disp = 1
        conf.logger.info("func=messagewrapper:parse_zk msgmap=%s " % (str(self.msgmap)))

    def parse_lc(self):
        for k in self.util.cfg["msgmap"].keys():
            # msgmap[queue_name] = ( [1003,1004,2001],
            # [['subscriber1_ip',port],['subscriber2_ip',port]],redis_sharding_idx)
            self.msgmap[k] = [self.util.cfg["msgmap"][k]["msg-ice"],
                              self.util.cfg["msgmap"][k]["sub-ice"],
                              self.util.cfg["msgmap"][k]["shard-idx"]]
            for m in self.msgmap[k][0]:
                if not self.msg2key.has_key(m):
                    self.msg2key[m] = set()
                self.msg2key[m].add(k)
            for m in self.msgmap[k][1]:
                m.append(None)     # subscriber's Ice communication
                m.append(None)     # subscriber's Ice proxy
                m.append(0)        # failed Ice request counter(used for retry subscriber by some frequency)

if __name__ == "__main__":
    time.sleep(10000)
    pass
