import time, sys, traceback, cmd
from kazoo.client import KazooClient

MQ_PATH = "/3g/ice/x2/MqService/"
REDIS_PATH = MQ_PATH + "redis"
MSGMAP_PATH = MQ_PATH + "msgmap"
SRV_PATH = MQ_PATH + ".service_nodes"

SRCZK='srczk'
DSTZK='dstzk'

g_dict = dict()


def setup_zk_client(addr, port, target=DSTZK):
    print "\t Zookeeper address is %s:%s" %(addr, port)
    try:
        zk = KazooClient()
        zk.start()
        g_dict[target] = zk
        print "\t zk client obj: %s" % zk
    except:
        print traceback.format_exc()
        print "set zk addr failed, addr is %s" %(addr)
    return zk

def get_zk_cli(target=DSTZK):
    if target in g_dict:
        return g_dict[target]

def create_zk_path(zk, path):
    print "Create path %s" % path
    zk.ensure_path(path)

def get_zk_info(zk, child):
    node = zk.get_children(MQ_PATH + child, None, False)
    for i in range(0, len(node)):
        print "\t%d. %s" %(i+1, node[i])
    return node

def is_existed(zk, path):
    zk.exists(path)

def get_queue_info(zk, title):
    data = zk.get(MSGMAP_PATH + "/" + title)
    print "queue data is: %s" % (str(data[0]))

def get_redis_info(zk):
    r = zk.get_children(REDIS_PATH)
    for i in range(0,len(r)):
        print "\tRedis Shard Info:  %s"%(r[i])
    return r

def set_redis_shard(zk, title, msglist):
    print "\n--------------------\nredis sharding info: \n"
    path = MSGMAP_PATH + "/"+title
# FIXME
#    if not is_existed(zk, path):
#        print "\n%s node is not existed"%path
#        return -1
    r1 = get_redis_info(zk)
    shard = int(raw_input("please select redis sharding:\n"))
    if (shard-1) > len(r1):
        print "you choose wrong shard"
        return -1
    data = [r1[shard-1], msglist]
    zk.set(path, str([r1[shard-1], msglist]))
    return 0


def zexit():
    print "exit admin tools..."
    if get_zk_cli(SRCZK):
        get_zk_cli(SRCZK).stop()

    if get_zk_cli(DSTZK):
        get_zk_cli(DSTZK).stop()
    sys.exit()

def init_zk(zk):
    create_zk_path(zk, MSGMAP_PATH)
    create_zk_path(zk, REDIS_PATH)
    create_zk_path(zk, SRV_PATH)

def sync_zk_msgmap(srczk, dstzk):
    try:
            msgmap = srczk.get_children(MSGMAP_PATH)
            for i in msgmap:
                    path = MSGMAP_PATH+"/"+i
                    data = str(list(srczk.get(path))[0])
                    print "create node: %s data=%s" % (path, str(data))
                    dstzk.ensure_path(path)
                    dstzk.set(path, data)
                    print "create node: %s" % (path+"/.service_nodes")
                    dstzk.ensure_path(path+"/.service_nodes")
    except:
            print traceback.format_exc()


def sync_zk_redis(srczk ,dstzk):
    try:
        shard = srczk.get_children(REDIS_PATH)
        for i in shard:
                dstzk.ensure_path(REDIS_PATH + "/" + i)
                print "create node: %s" % (REDIS_PATH +"/"+i)
                dstzk.ensure_path(REDIS_PATH + "/" + i + "/.service_nodes")
                try:
                        r = srczk.get_children(REDIS_PATH +"/"+i+"/.service_nodes")
                        print "==========rrrr=" + r
                except:
                        continue
                for j in r:
                        print "create node: %s" % (REDIS_PATH + "/"+i+"/.service_nodes/"+j)
                        dstzk.ensure_path(REDIS_PATH + "/" + i + "/.service_nodes")
    except:
            print traceback.format_exc()

def sync_zk(srczk, dstzk):
    init_zk(dstzk)
    sync_zk_redis(srczk, dstzk)
    sync_zk_msgmap(srczk, dstzk)

def add_msg_map(zk, title, msglists):
    msgmap = zk.get_children(MSGMAP_PATH)
    print "\n current message queue info\n"
    for i in range(0,len(msgmap)):
        print "\t"+str((int(i)+1))+". "+str(msgmap[i])
    if title in msgmap:
        print "title already exist, create failed"
        return  -1
    zk.ensure_path(MSGMAP_PATH+"/"+title)
    zk.ensure_path(MSGMAP_PATH+"/"+title+"/.sercice_nodes")
    msglist = eval(msglists)
    set_redis_shard(zk, title, msglist)

def modify_msg_map(zk, title, msglists):
    msgmap = zk.get_children(MSGMAP_PATH)
    print "\n current message queue info\n"
    for i in range(0,len(msgmap)):
        print "\t"+str((int(i)+1))+". "+str(msgmap[i])
    if title in msgmap:
        msglist = eval(msglists)
        set_redis_shard(zk, title, msglist)


class ZookeeperSet(cmd.Cmd):
    """ subclass of cmd.Cmd """

    def do_init(self, line):
        "   Init zookeeper tree structure."
        init_zk(get_zk_cli())

    def do_addMsgMap(self, line):
        "   add messge map into zookeeper. Example: addMsgMap feed [10001, 100002]"
        (title, msglists) = line.split(" ", 1)
        add_msg_map(get_zk_cli(), title, msglists)

    def do_modifyMsgMap(self, line):
        "   modify messge map into zookeeper. Example: modifyMsgMap feed [10001, 100002]"
        (title, msglists) = line.split(" ", 1)
        modify_msg_map(get_zk_cli(), title, msglists)


    def do_queueinfo(self, title):
        "   Get queue info from zookeeper. Example: queueinfo feed/message/search"
        get_queue_info(get_zk_cli(), title)

    def do_gredis(self, line):
        "   Get redis info from zookeeper."
        get_redis_info(get_zk_cli())

    def do_sync(self, line):
        "   MQ zookeeper [sync src dst] Example: sync 10.4.16.206:2181 127.0.0.1:2181"
        (src, dst) = line.split(" ", 1)
        print "params is  %s, %s"%(src, dst)
        # TODO  add ip address check.
        (srcaddr, srcport) = src.split(":", 1)
        (dstaddr, dstport) = dst.split(":", 1)

        setup_zk_client(srcaddr, srcport, SRCZK)
        setup_zk_client(dstaddr, dstport, DSTZK)
        sync_zk(get_zk_cli(SRCZK), get_zk_cli())

    def do_queue2redis(self, line):
        "   Set redis sharding for MQ. Example: queue2redis feed 10004,10005"
        (title, msglist) = line.split(" ", 1)
        set_redis_shard(get_zk_cli(), title, msglist)

    def do_EOF(self, line):
        zexit()
        return True

    def default(self, line):
        print_main_menu()
        return cmd.Cmd.default(self, line)



""" #################### Prompt display mehtods ####################### """
def print_main_menu():
    print ""
    print " Starting MqService Admin toole..."
    print ""
    print "   gredis: get zookeeper redis "
    print "   queue2redis: set zookeeper redis for specific queue "
    print "   3. set zookeeper msgmap "
    print "   sync src dst: mq zookeeper sync"
    print "   init: init zookeeper tree structure"
    print "   EOF: exit "
    print ""


if __name__ == "__main__":

    print "main..."
    setup_zk_client("127.0.0.1", "2181")
    ZookeeperSet().cmdloop()

