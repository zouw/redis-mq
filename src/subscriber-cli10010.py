import sys
from zkclient import ZKClient, zookeeper, watchmethod
sys.path.append("../dep/ice/")
import Ice
Ice.loadSlice("../slice/mq.ice")
import com.renren.x2.mq
mq=com.renren.x2.mq



class MqServiceI(mq.MqService):
	def forwardMq(self, mid, msg, current=None):
		print msg
		return 1

if __name__ == "__main__":
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)
        zk = ZKClient("localhost:2181")
        try:
                path = zk.create("/3g/ice/x2/MqService/msgmap/test/.service_nodes/127.0.0.1:10015", flags=zookeeper.EPHEMERAL)
        except:
                pass
        try:
            ic = Ice.initialize(sys.argv)
            addr ="default -h %s -p %d -t 200" % ('127.0.0.1',10015)
            adapter   = ic.createObjectAdapterWithEndpoints( "MqService", addr)
            adapter.add(MqServiceI(), ic.stringToIdentity("MqService"))
            adapter.activate()
            ic.waitForShutdown()
        except Exception,x:
            conf.logger.error(str(x))
            
