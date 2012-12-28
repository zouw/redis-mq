import socket,os,struct,random,time

import sys
sys.path.append("../dep/ice/")
import Ice
Ice.loadSlice("../slice/mq.ice")
import com.renren.x2.mq

mq=com.renren.x2.mq

ic=Ice.initialize(sys.argv)
base=ic.stringToProxy("MqService:tcp -h 10.4.16.206 -p 30002 -t 200")
m=mq.MqServicePrx.checkedCast(base)
if not mq:
	raise RuntimeError("Invalid proxy")
#msglist=[10000,10001,20001,20004,21004,21002,21001,10023]
msglist=[21001]
count=100000000

while True:
	if count==0:
		break
	count -= 1
	msgid = msglist[int(random.random() *  1000) % len(msglist)]
        print msgid
        data ="{'name':'zach','gentle':'male','haha':'hehe'}"

	try:
		r = m.sendMq(int(msgid), data)
		print r
                time.sleep(0.2)
		continue
	except Exception, x:
		print str(x)
                time.sleep(0.2)
		continue

