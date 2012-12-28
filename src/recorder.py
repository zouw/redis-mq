#!/usr/bin/env python
# author:zouwan@gmail.com
# create time : 2012.11.28
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
import os,struct,json,time, redis,struct,traceback
import conf, constant, utils,signal
from zkwrapper import zkWrapper
from multiprocessing import Process
import sys
sys.path.append("../dep/ice/")
import Ice
Ice.loadSlice("../slice/mq.ice")
import com.renren.x2.mq
mq = com.renren.x2.mq

#===============================================
# msg record
#   1.serialize msg to redis
#   2.one queue per subscriber
#   3.one subscribe fetch msg in queue, and msg
#     been removed
#===============================================
class Recorder(Process):
        """
        write req to 0~n lists in redis.
        """
        def __init__(self,ip,port):
            self.ip=ip
            self.port=port
            super(Recorder,self).__init__()

        def run(self):
            signal.signal(signal.SIGUSR1, utils.signal_pause)
            signal.signal(signal.SIGUSR2, utils.signal_restore)

            try:
                ic = Ice.initialize(sys.argv)
                addr ="default -h %s -p %d -t 200" % (self.ip,self.port)
                adapter   = ic.createObjectAdapterWithEndpoints( "MqServiceAdapter", addr)
                adapter.add(MqServiceI(), ic.stringToIdentity("MqService"))
                adapter.activate()
                conf.logger.info("===========server start ip:%s port:%d=============" %(str(self.ip), self.port))
                ic.waitForShutdown()
            except Exception,x:
                conf.logger.error(str(x))

class MqServiceI(mq.MqService):
        def __init__(self):
            self.util = None

        def sendMq(self, mid, msg, current=None):
            if self.util == None:
                self.util = utils.GlobalVar()
            ret = self.record_msg(mid, msg)
            return int(ret)

        def pre_record_msg(self, mid, buf):
            """
            pre_record_msg
            use mid to find lists in self.msg2key and filter not needed msg
            @mid message id
            """
            self.util.msginfo.rcv_msg_num += 1
            if mid not in self.util.msginfo.msg2key.keys():
                conf.logger.info("mid(%d) not in msglist. data=%s" % (mid, buf))
                return (False, None)
            return (True, struct.pack("i%ds" % len(buf), mid, buf))

        def record_msg(self, mid, buf):
            """
            using roundrobin save msg to redis random servers(must master instance)
            """

            # system pause. return 2.
            if utils.g_state == 1:   return 2

            conf.logger.debug("recieve_msg_num_uniq=%d record_msg_num_tot=%d mid=%d buf=%s" %
                              (self.util.msginfo.rcv_msg_num, self.util.msginfo.rec_msg_num, mid, buf))
            (ret,data) = self.pre_record_msg(mid, buf)

            if not ret: return 1

            # save buf to subscribers' list, when all redis is not available
            # process will retry util one redis server restore.

            for title in self.util.msginfo.msg2key[mid]:
                rdx = self.util.msginfo.msgmap[title][2]
                conf.logger.debug(str(self.util.msginfo.msgmap[title]))
                if not self.util.redis.conn.has_key(rdx):
                    conf.logger.debug(str(self.util.redis.conn))
                    conf.logger.error("not find redis sharding key %s" %(rdx))
                    continue

                idx = self.util.redis.conn[rdx][0]
                bSuccess = False
                while True:
                    retry_num = 0
                    for i in [0,1,2]:
                        try:
                            self.util.redis.conn[rdx][1][idx][1].rpush(title, data)
                            self.util.msginfo.rec_msg_num += 1
                            bSuccess = True
                            conf.logger.debug("save success for mid=%d title=%s rdx=%s idx=%d" % (mid, title, rdx, idx))
                            break
                        except Exception,x:
                            retry_num += 1
                            conf.logger.error("%d try,redis(rdx=%s,idx=%d,%s) data=%s  desc:%s " %
                                              (retry_num, rdx, idx,str(self.util.redis.conn[rdx]),
                                               buf,traceback.format_exc()))
                            self.util.redis.retry_connect_rdx(rdx)

                        # if all retry failed, sleep and retry again
                        if not bSuccess:
                            time.sleep(constant.SLEEP_REC_NO_REDIS)
                            conf.logger.error("save failed for mid=%d, data=%s, redis idx=%s" % (mid, data, rdx))
                            continue
                    break
            return 0



if __name__ == "__main__":
    b=Recorder("127.0.0.1", 30009)
    b.start()
    b.join()
#    if b.util.cfg["config"]["server"]["auto"]=="true":
#        host = b.util.get_host_ip_port()
#    else:
#        ip = b.util.cfg["config"]["server"]["ip"]
#        port = b.util.cfg["config"]["server"]["port"]
#        host = (ip,port)
#    b.util.zk.register(host[0], host[1], ".service_nodes")
#    b.join()
    time.sleep(100)
#    print "success"
