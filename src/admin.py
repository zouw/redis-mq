import time,sys,traceback
from zkclient import ZKClient, zookeeper, watchmethod
import constant

MQ_PATH = "/3g/ice/x2/MqService/"
g_var = dict()

def zcreate(zk, path, data, flags):
    try:
        path = zk.create(path, "", flags)
        print "zcreate %s success." % (path)
    except Exception,x:
        print "zcreate %s failed. desc:%s" % (path, str(x))

def print_main_menu():
    print ""
    print " Starting MqService Admin toole..."
    print ""
    print "   1. set zookeeper address[default:localhost:2181]"
    print "   2. set zookeeper redis "
    print "   3. set zookeeper msgmap "
    print "   0. exit "
    print ""

def print_zk_menu():
    print ""
    print " Zookeeper Address Setting.."
    print " "

def set_zk_addr():
    g_var['zk'] = set_zk_addr_ex()

def set_zk_addr_ex():
    print "\tZookeeper Address Setting.."

    while True:
        addr = raw_input("\tplease input zk addr(eg:127.0.0.1:2181), \n\t'q' exit :\n")
        if addr == 'q':
            break
        try:
            print "set zk addr success, addr is %s" %(addr)
            return ZKClient(addr)
            break
        except:
            print traceback.format_exc()
            print "set zk addr failed, addr is %s" %(addr)

def zexit():
    print "exit admin tools..."
    sys.exit()

def zookeeper_set():
    pass

def get_queue_info(title):
    data = g_var['zk'].get(MQ_PATH+"msgmap/"+title)
    print "queue data is: %s" % (str(data[0]))


def get_redis_info():
    r = g_var['zk'].get_children(MQ_PATH+"redis")
    for i in range(0,len(r)):
        print "\t%d. %s"%(i+1, r[i])
    return r

def set_redis_shard(title, msglist):
    print "\n--------------------\nredis sharding info: \n"
    r1 = get_redis_info()
    shard = int(raw_input("please select redis sharding:\n"))
    if (shard-1) > len(r1):
        print "you choose wrong shard"
        return -1
    data = [r1[shard-1], msglist]
    g_var['zk'].set(MQ_PATH+"msgmap/"+title, str([r1[shard-1], msglist]))
    return 0

def set_redis_info():
    redis = get_redis_info()
    print "\n----------------------------\n"
    print "\t1 add new shard"
    print "\t2 add new redis addr"
    id = raw_input("please select op :\n")
    if id=='1':

        g_var['zk'].create(MQ_PATH+"redis/"+name, "")
        g_var['zk'].create(MQ_PATH+"redis/"+name+"/.service_nodes", "")
    if id=='2':
        idx = raw_input("input redis shard name idx:\n")
        addr = raw_input("input redis addr(eg:127.0.0.1:3666):\n")
        g_var['zk'].create(MQ_PATH+"redis/"+name+"/.sercice_nodes/"+redis[int(idx)-1], "")

def set_msg_map():
    while True:
        msgmap = g_var['zk'].get_children(MQ_PATH+"msgmap")
        print "\n current message queue info\n"
        for i in range(0,len(msgmap)):
            print "\t"+str((int(i)+1))+". "+str(msgmap[i])
        print "\n----------------------------"
        print "\t1. add new message queue"
        print "\t2. modify queue subscribed message"
        print "\tq. return up"
        id = raw_input("please select op :\n")
        if id == 'q':
            return
        if id == '1':
            title = raw_input("please input queue name :\n")
            if title in msgmap:
                print "title already exist, create failed"
                continue
            g_var['zk'].create(MQ_PATH+"msgmap/"+title, "")
            g_var['zk'].create(MQ_PATH+"msgmap/"+title+"/.sercice_nodes", "")
            msglists = raw_input("please input message list, split by ','(example: 10001,10002,10003 :\n")
            msglist = eval(msglists)
            set_redis_shard(title, msglist)
            continue
        if id == '2':
            try:
                qidx = int(raw_input("please select message queue idx:\n"))
                print "you select %s" % msgmap[qidx-1]
                if qidx>len(msgmap):
                    print "wrong message queue idx\n"
                    continue
                data = g_var['zk'].get(MQ_PATH+"msgmap/"+msgmap[qidx-1])[0]
                if data=='':
                    data = [get_redis_info()[0],[]]
                else:
                    data = eval(data)
                data[1] = list(data[1])
                print str(data)
                print "%s subscribed message list: %s\n" % (msgmap[qidx-1], str(data))
                print "\t1.append a message"
                print "\t2.set message list"
                print "\t3.set redis sharding "
                print "\t4.clear queue info "
                #                print "\t5.delete queue "
                print "\tq. return up"
                j = raw_input("please select op idx:\n")
                if j == '1':
                    d_raw = raw_input("please input message list, split by , :\n")
                    d = list(eval(d_raw))
                    for k in d:
                        if k not in data[1]:
                            data[1].append(k)
                    g_var['zk'].set(MQ_PATH+"msgmap/"+msgmap[qidx-1], str(data))
                    data = g_var['zk'].get(MQ_PATH+"msgmap/"+msgmap[qidx-1])
                    print "after append data is: %s" % (str(data[0]))
                    continue
                if j == '2':
                    d_raw = raw_input("\nplease input queue, split by ',' :\n")
                    d = list(eval(d_raw))
                    print "make sure data is right (y/n)"
                    select = raw_input()
                    if select == 'y':
                        data[1] = d
                        g_var['zk'].set(MQ_PATH+"msgmap/"+msgmap[qidx-1], str(data))
                        data = g_var['zk'].get(MQ_PATH+"msgmap/"+msgmap[qidx-1])
                        print "after append data is: %s" % (str(data[0]))
                    else:
                        print "you choose cancel"
                        continue
                if j == '3':
                    data = eval(g_var['zk'].get(MQ_PATH+"msgmap/"+msgmap[qidx-1])[0])
                    set_redis_shard(msgmap[qidx-1], data[1])
                    g_var['zk'].set(MQ_PATH+"msgmap/"+msgmap[qidx-1], str(data))
                    data = g_var['zk'].get(MQ_PATH+"msgmap/"+msgmap[qidx-1])
                    print "after set sharding data is: %s" % (str(data[0]))
                    continue
                if j=='4':
                    g_var['zk'].set(MQ_PATH+"msgmap/"+msgmap[qidx-1], "")
                    get_queue_info(msgmap[qidx-1])
                    continue
                if j=='5':
                    path = MQ_PATH+"msgmap/"+msgmap[qidx-1]
                    print path
                    g_var['zk'].delete(path+"/.service_nodes")
                    g_var['zk'].delete(path)
                    continue
            except:
                print traceback.format_exc()
            if id == 'q':
                return
            print "you input wrong option, please input your choice\n"

def sync_zk_msgmap(src,dst):
    try:
            print MQ_PATH+"msgmap"
            msgmap = src.get_children(MQ_PATH+"msgmap")
            for i in msgmap:
                    path = MQ_PATH+"msgmap/"+i
                    data = str(list(src.get(path))[0])
                    print "create node: %s data=%s" % (path, str(data))
                    zcreate(dst, path, data, 0)
                    dst.set(path, data)
                    print "create node: %s" % (path+"/.service_nodes")
                    zcreate(dst, path+"/.service_nodes", "", 0)
    except:
            print traceback.format_exc()

def sync_zk_redis(src,dst):
    try:
        shard = src.get_children(MQ_PATH+"redis")
        for i in shard:
                zcreate(dst, MQ_PATH+"redis/"+i, "",0)
                print "create node: %s" % (MQ_PATH+"redis/"+i)
                zcreate(dst, MQ_PATH+"redis/"+i+"/.service_nodes", "",0)
                try:
                        r = src.get_children(MQ_PATH+"redis/"+i+"/.service_nodes")
                except:
                        continue
                for j in r:
                        print "create node: %s" % (MQ_PATH+"redis/"+i+"/.service_nodes/"+j)
                        zcreate(dst, MQ_PATH+"redis/"+i+"/.service_nodes/"+j, "", 0)
    except:
            print traceback.format_exc()

def init_zk():
    zcreate(g_var['zk'], "/3g","", 0)
    zcreate(g_var['zk'], "/3g/ice","", 0)
    zcreate(g_var['zk'], "/3g/ice/x2","", 0)
    zcreate(g_var['zk'], "/3g/ice/x2/MqService","", 0)
    zcreate(g_var['zk'], MQ_PATH+"msgmap","", 0)
    zcreate(g_var['zk'], MQ_PATH+"redis","",0)
    zcreate(g_var['zk'], MQ_PATH+".service_nodes","",0)

def sync_zk():
    init_zk()
    print "Set source zk addr"
    zk_src = set_zk_addr_ex()
    #    zk_src = ZKClient("10.4.16.206:2181")
    print "Set target zk addr"
    zk_dst = set_zk_addr_ex()
    #    zk_dst = ZKClient("127.0.0.1:2181")
    sync_zk_redis(zk_src, zk_dst)
    sync_zk_msgmap(zk_src, zk_dst)


def print_main_menu():
    print ""
    print " Starting MqService Admin toole..."
    print ""
    print "   1. set zookeeper address[default:localhost:2181]"
    print "   2. set zookeeper redis "
    print "   3. set zookeeper msgmap "
    print "   4. mq zookeeper sync"
    print "   5. init zookeeper tree structure"
    print "   0. exit "
    print ""

main = {"0":zexit,"1":set_zk_addr, "2":set_redis_info, "3":set_msg_map,
        "4":sync_zk, "5":init_zk}

if __name__ == "__main__":
    zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)
    g_var['zk'] = ZKClient("localhost:2181")
    while True:
        try:
            print_main_menu()
            op = raw_input("please select menu option: \n")
            main[op]()
            time.sleep(0.2)
        except Exception,x:
            print x
