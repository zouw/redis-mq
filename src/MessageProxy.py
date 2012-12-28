#!/usr/bin/env python
#
# author:zouwan@gmail.com
# create time : 2012.11.29
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
#
# Feature (done)
#  1. async message dispatch
#  2. Ice interface for publisher
#  3. dynamic load subscriber's Ice Proxy
#  4. process manager(when main process exit, subprocess exit too)
#  0. make sure one subscriber's message will only write to
#     one redis nodes(1master, 2slave) urgent
#  1. get available redis server through zookeeper
#     -->(redis can be monitored by zk, but mq has retry
#         logic and don't care)
#  2. auto discover subscriber addr through zookeeper
#     -->(useful, can help keep consistency, replace msgmap.conf)
#
# Todo
#  3. admin ice interface to get realtime statistic info
#
from multiprocessing import Process
import recorder,dispatcher,conf,constant,utils,time,os,signal

class MessageProxy:
        def __init__(self):
            # init subscribe info
            self.process = dict()
            self.process[0] = None   # recorder process
            self.process[1] = dict() # dispatcher process
            self.ip = "127.0.0.1"
            self.port = 30002
            self.util = utils.GlobalVar()


        def check(self):
            """
            check if any worker process( recorder/dispatcher ) not avail
            and restart it
            """
            # check recorder process
            flag = False
            if not self.process[0][0].is_alive():
                self.process[0][1] = self.process[0][0].start()
                flag = True

            for i in self.process[1]:
                if not self.process[1][i][0].is_alive():
                    self.process[1][i][1] = self.process[1][i][0].start()
                    flag = True
            if flag:
                self.write_pid()

        def write_pid(self):
            # write main process's pid to file pid
            old = constant.PID_FILE_PATH
            new = old+"-"+str(int(time.time()))
            try:
                os.rename(old,new)
            except:
                pass
            pid_file = open(old, 'w')

            pid_file.write(str(os.getpid())+"\n")
            pid_file.write(str(self.process[0][1])+"\n")

            for p in self.process[1]:
                pid_file.write(str(self.process[1][p][1])+"\n")
            pid_file.close()

        # server start
        def run(self):
            """
            new dispatcher process will be created if needed. (such as new added title)
            """
            while True:
                # recieve pause signal, stop working
                if utils.g_state==1:
                    time.sleep(constant.SLEEP_POLLING)
                    continue

                # recieve zk watch event
                if self.util.update_disp == 1:
                    self.util.update_disp = 0
                    self.record_message()
                    self.dispatch_backends()
                    r_num = 0 if not self.process[0] else 1
                    self.write_pid()
                    # print current process info
                    # example: Starting 127.0.0.1:30002 success(1 main 1 recorder, 2 dispatcher-{
                    # 'search-0-127.0.0.1-36667': (<Dispatcher(Dispatcher-3, started)>, 26274) ,
                    # 'search-0-127.0.0.1-36666': (<Dispatcher(Dispatcher-4, started)>, 26278) })
                    conf.logger.info('Starting %s:%d success(1 main(pid:%d) %d recorder-%s), %d dispatcher-%s)' %
                                     (self.util.host,self.util.port,os.getpid(),r_num,
                                      str(self.process[0]),len(self.process[1]), str(self.process[1])))
                else:
                    time.sleep(constant.SLEEP_POLLING)
                    self.check()

        def record_message(self):
            """
            start record process, only one process record
            @ret True
            """
            if self.process[0]==None:
                r = recorder.Recorder(self.ip,self.port)
                r.start()
                self.process[0]=[r, r.pid]
                conf.logger.debug("start new recorder process")

        def dispatch_backends(self):
            """
            start dispatch process, one process per Client [ip,port]  ->  msg1,msg2 ]
            1. dynamic forked process to dispatch message in message list
            2. one process responsible for one redis instance one message list
            """
            conf.logger.debug(str(self.util.msginfo.msgmap))
            for key in self.util.msginfo.msgmap.keys():
                rdx = self.util.msginfo.msgmap[key][2]
                if not self.util.redis.conn.has_key(rdx):
                    conf.logger.warn("func=dispatch_backends err=redis_shard_(%s)_not_exist" %(rdx))
                    continue
                redis = self.util.redis.conn[rdx][1]
                conf.logger.debug("func=dispatch_backends key=%s rdx=%s redis=%s" %(key, rdx, str(redis)))
                for idx in range(0, len(redis)):
                    try:
                        token = "-".join([key, rdx, redis[idx][0][0], str(redis[idx][0][1])])
                        if self.process[1].has_key(token):
                            continue
                        d = dispatcher.Dispatcher(key, rdx, idx)
                        d.start()
                        self.process[1][token] = [d, d.pid]
                    except Exception, x:
                        conf.logger.error("func=dispatch_backends" + str(x))



# main entry
if __name__ == "__main__":
    # MessageProxy
    prx = MessageProxy()

    signal.signal(signal.SIGUSR1, utils.signal_pause)
    signal.signal(signal.SIGUSR2, utils.signal_restore)

    if prx.util.check_port(prx.util.host, prx.util.port) == 0:
        # register mq zk node
        if prx.util.cfg["config"]["server"]["auto"]=="true":
            host = prx.util.get_host_ip_port()
        else:
            ip = prx.util.cfg["config"]["server"]["ip"]
            port = prx.util.cfg["config"]["server"]["port"]
            host = (ip,port)
        (prx.ip,prx.port) = (host[0], host[1])
        prx.util.zk.register(host[0], host[1])

        # starting
        prx.run()

        # manager sub process
        prx.process[0][0].join()
        for i in prx.process[1].keys():
            prx.process[1][i][0].join()
