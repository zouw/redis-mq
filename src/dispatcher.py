#!/Usr/bin/env python
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
# Todo
#   1. catch subscriber timeout exception (done)
#   2. test when mq runs as cluster  (done)
#
import os,time,socket,redis,struct,traceback,threading
import conf,utils,constant,thread
from multiprocessing import Process
import sys,signal
sys.path.append("../dep/ice/")
import Ice
Ice.loadSlice("../slice/mq.ice")
import com.renren.x2.mq
mq = com.renren.x2.mq

class Dispatcher_thread(threading.Thread):
        """
        #===============================================
        # msg dispatch
        #
        #   breif: 	dispatch msg to subscribed client
        #
        #   1.one proccess <-> one msglist and one redis instance
        #   2.one msglist  <-> one service
        #   3.blocking at redis blpop, once msg been produced,
        #     Dispatcher pop the msg and assure send to client
        #   4.only one subscriber of msglist can recv one copy
        #   5.when redis not available: process sleep and retry
        #   6.when all subscribers not available: process sleep
        #     and retry
        #===============================================
        """
        def __init__(self,key,rdx,idx):
            self.key  = key
            self.rdx  = rdx
            self.idx  = idx
            super(Dispatcher_thread,self).__init__()

        def pre_handler(self, redis):
            """
            pre handler
            1. output info of current dispatch process(include backlog in redis)

            @key message list name
            @idx redis server index
            @ret tuple of ( redis_instance, list_of_subscriber_socket_connection )
            """
            global g_util
            (key,rdx,idx) = (self.key,self.rdx,self.idx)

            # output backlog message in redis(will replay in fifo)
            try:
                conf.logger.info("start dispatcher key:'%s' redis(%s:%d) backlog:%d" %
                                 (key,redis[0][0],redis[0][1], redis[1].llen(key)))
            except Exception, x:
                conf.logger.error("start dispatcher key:'%s' redis(%s:%d) backlog:unknown" %
                                 (key,redis[0][0],redis[0][1]))

        def get_msg(self, conn, rdx, idx, key):
            """
            get_msg
            1. get and pop one message from redis queue(key)
            2. unpack message body
            return (mid, unpacked message data, status(0:suc,1:err)
            """
            global g_util
            for i in [0,1]:
                try:
                    (r_key, r_data) = conn[1].blpop(key, 0)
                    (mid, data) = struct.unpack("i%ds" % (len(r_data)-4), r_data)
                    conf.logger.warn('fun=dispatcher:get_msg threadid=%d ' % (thread.get_ident()))
                    return (mid, data, 0)
                except Exception, x:
                    g_util.redis.retry_connect_rdx_idx(rdx, idx)
                    conf.logger.warn('fun=dispatcher:get_msg desc=%s ' % (str(x)))
                    time.sleep(constant.SLEEP_DISP_NO_REDIS)
                    return (0, None, 1)

        def run(self):
            self.handler()

        def handler(self):
            """
            0. redis is sharding , each sharding has at least two redis instance
            1. one dispatcher process will visit only one sharding of redis
            2. title(key) is the message list for this subscriber
            3. when all redis not available in one redis sharding,dispatcher retry and sleep
            4. when one subscriber not available, message will send to other subscribers
            5. when all subscriber not available, process will retry and sleep
            """
            global g_util
            (key, rdx, idx) = (self.key, self.rdx, self.idx)

            conf.logger.debug("threadid=%d key=%s rdx=%s idx=%d" %(thread.get_ident(), self.key, self.rdx, self.idx))

            # [[['ip1',port1],redis object1],['ip2',port2],redis object2]]
            redis = g_util.redis.conn[rdx]

            # output history info of this title(key)
            self.pre_handler(redis[1][idx])

            # first connect subscriber
            subscribe = g_util.retry_connect_subscriber(key)

            rrobin = -1
            retry_interval = []
            # one dispatcher must have no more than 100 subscriber
            for i in range(0, constant.MAX_SUBS_ONE_DISP):
                retry_interval.append(0)

            while True:
                try:
                    # blocking get msg from redis
                    (mid, data, flag)=self.get_msg(redis[1][idx],rdx, idx, key)
                    conf.logger.debug('get_msg=(%s,%s,%d)' % (mid, data, flag))

                    if flag == 1:
                        continue
                    count = 0
                    while True:
                        if utils.g_state == 1:
                            try:
                                if not data==None:
                                    redis[1][idx][1].rpush(key, data)
                                    conf.logger.info("signal pause, repush data to queue, " +
                                                     "rdx=%s idx=%d title=%s data={%s} len=%d" %
                                                     (rdx, idx, key, data, redis[1][idx][1].llen(key)))
                                    data = None
                            except Exception,x:
                                conf.logger.error("repush failed. signal pause repush failed" +
                                                  "rdx=%s idx=%d title=%s desc:%s data={%s} len=%d" %
                                                  (rdx, idx, key, str(x),data, redis[1][idx][1].llen(key)))
                                data = None
                                time.sleep(constant.SUB_RETRY_INTEVAL)
                            continue
                        sublen = len(g_util.msginfo.msgmap[key][1])
                        if sublen == 0:
                            time.sleep(constant.SLEEP_DISP_NO_SUB)
                            continue
                        rrobin = 0 if rrobin==sublen else rrobin+1
                        tryidx = rrobin % sublen
                        conf.logger.debug("threadid=%d rrobin=%d tryidx=%d data=%s len=%d" %
                                          (thread.get_ident(), rrobin, tryidx,str(g_util.msginfo.msgmap[key][1]), len(g_util.msginfo.msgmap[key][1])))
                        try:
                            sip   = g_util.msginfo.msgmap[key][1][tryidx][0]
                            sport = g_util.msginfo.msgmap[key][1][tryidx][1]
                            if g_util.msginfo.msgmap[key][1][tryidx][3] == None:
                                raise Exception("subscriber %s:%d not available"%
                                                  (g_util.msginfo.msgmap[key][1][tryidx][0],
						   g_util.msginfo.msgmap[key][1][tryidx][1]))
                            ret = g_util.msginfo.msgmap[key][1][tryidx][3].forwardMq(mid, data)
                            g_util.msginfo.msgmap[key][1][tryidx][4] += 1
                            conf.logger.debug("forward success rrobin=%d tryidx=%d data=%s ret=%s" %
                                              (rrobin, tryidx, data, str(ret)))
                            flag = True
                            break
                        except Exception, x:
                            conf.logger.error("dispatcher %s" % (traceback.format_exc()))
                            count    += 1
                            if retry_interval[tryidx] == 0:
                                retry_interval[tryidx] = int(time.time())
                                g_util.msginfo.msgmap[key][1][tryidx][3] = None
                            if time.time()-retry_interval[tryidx] > constant.SUB_RETRY_INTEVAL:
                                retry_interval[tryidx] = 0
                                g_util.retry_connect_subscriber_idx(key, tryidx)
                        if count == sublen:
                            count = 0
                            conf.logger.error("no available subscribers, sleep %ds %s" %
                                              (constant.SLEEP_DISP_NO_SUB, str(g_util.msginfo.msgmap[key][1])))

                            # all subscriber not available, sleep xs and retry ...
                            time.sleep(constant.SLEEP_DISP_NO_SUB)
                            g_util.retry_connect_subscriber(key)
			    conf.logger.debug ("===%s===" % (str(g_util.msginfo.msgmap[key][1])))
                except Exception,x:
                    conf.logger.error("dispatcher fatal error: %s"%(traceback.format_exc()))


g_util=None
class Dispatcher(Process):
        def __init__(self,key,rdx,idx):
            self.key  = key
            self.rdx  = rdx
            self.idx  = idx
            super(Dispatcher,self).__init__()

        def run(self):
            global g_util
            if g_util == None:
                g_util = utils.GlobalVar()  # attension, must not initial in constructor
                                               # otherwise, dispatcher will not recieve zk event

            signal.signal(signal.SIGUSR1, utils.signal_pause)
            signal.signal(signal.SIGUSR2, utils.signal_restore)
            for i in range(0, constant.THREAD_PER_DISP):
                tid = Dispatcher_thread(self.key, self.rdx, self.idx)
                tid.start()
                conf.logger.debug("key=%s rdx=%s idx=%d starting thread %s" %(self.key,self.rdx,self.idx,tid))

            while True:
                time.sleep(5)

if __name__ == "__main__":
    r = Dispatcher("feed", "1", 0)
    r.start()
    print "success"
