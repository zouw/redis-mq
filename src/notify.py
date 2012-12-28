# coding: utf-8
import httplib,time
import smtplib,sys,redis
import time,sys,traceback
from zkclient import ZKClient, zookeeper, watchmethod
from email.mime.text import MIMEText

zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)
MQ_PATH = "/3g/ice/x2/MqService/"
ZK_ADDR  = "127.0.0.1:2181"
ALERT_MSG_NUM = 200

# exmaple
#    python notify.py sms 13810910291 mq\ restarting
#    python notify.py sms 13810910291 mq\ restarting
SMSURL = "10.3.19.199:8080"
SENDER = "3g_exception@renren-inc.com"
SERVER = "smtp.renren-inc.com"
USERNAME = "3g_exception"
PASSWORD = "3g.Exception"

STAT_MAIL_TO='wan.zou@renren-inc.com'
#STAT_MAIL_TO='wan.zou@renren-inc.com:wei.zhao@renren-inc.com:jie.liang@renren-inc.com:chuang.zhang@renren-inc.com:ming.yin@renren-inc.com:tao.ma@renren-inc.com;liqian@renren-inc.com:yongshuai.yan@renren-inc.com:wei.cui@renren-inc.com:qiangmin.sun@renren-inc.com:peng.jiang@renren-inc.com'

def alert():
        zk_dst = ZKClient(ZK_ADDR)
        stat = dict()
        redis_conn= []
        title = zk_dst.get_children(MQ_PATH+"msgmap")
        shard = zk_dst.get_children(MQ_PATH+"redis")
        for i in shard:
                child=zk_dst.get_children(MQ_PATH+"redis/"+i+"/.service_nodes")
                for k in child:
                        addr=k.split(':')
                        redis_conn.append(redis.Redis(addr[0], int(addr[1])))

        flag = False
        for i in title:
                for j in redis_conn:
                        if not stat.has_key(i):
                                stat[i]=0
                        len = j.llen(i)
                        stat[i] += len
        for i in stat:
                if stat[i]>ALERT_MSG_NUM:
                        flag=True
                        break

        if not flag:
                print "无积压消息"
                return

        subject = "MQ积压消息报警"
        body   = '\n'
        body   += time.asctime() + "\n"
        body   += 'Title  \t    Num' + "\n"
        for i in stat:
                body += 'title:' + i + '\t len:' + str(stat[i])+'\n'

        send_mail(STAT_MAIL_TO, subject, body)


# phone is list [13810910291,138222222]
def send_sms(phone, body):
        print str(phone)
        print str(body)
        phone_list = phone.split(':')
        print str(phone_list)
        conn=httplib.HTTPConnection(SMSURL)
        for n in phone_list:
                get_str='/api?cmd=sms&pdid=10050&mobile='+str(n)+'&msg='+body+';'
                print get_str
                conn.request('get',get_str)
        print conn.getresponse().read()
        conn.close()


# example:
# mail_to='wan.zou@renren-inc.com:wan.zou@renren-inc.com'
# mail_body=" alert. mq restarting "
def send_mail(mail_to, mail_subject, mail_body):
        mail_to_list = mail_to.split(':')
        mail_from=SENDER
        msg=MIMEText(mail_body)
        msg['Subject']=mail_subject
        msg['From']=SENDER
        msg['To']=mail_to
        print msg['To']
        msg['date']=time.strftime('%a, %d %b %Y %H:%M:%S %z')

        smtp=smtplib.SMTP()
        smtp.connect(SERVER)
        smtp.login(USERNAME, PASSWORD)
        smtp.sendmail(mail_from,mail_to_list,msg.as_string())
        smtp.quit()
        print 'send success'


if __name__=="__main__":
        if len(sys.argv)==1 or sys.argv[1]=='-h' or sys.argv[1]=='--help':
                print "example:  "
                print " \t python sms 发送短信"
                print " \t python mail 发送邮件"
                print " \t python alert MQ积压消息报警"
        elif sys.argv[1]=='sms':
                print "example: python notify.py sms 13810910291:121111111 MQ Restarting...'"
                print ''
                print str(sys.argv)
                send_sms(sys.argv[2], sys.argv[3])
                exit()
        elif sys.argv[1]=='mail':
                print "example: python notify.py mail wan.zou@renren-inc.com:wei.zhao@renren-inc.com 'MQ Restarting...' 'MQ Restarting...'"
                print ''
                send_mail(sys.argv[2], sys.argv[3], sys.argv[4])
                exit()
        elif sys.argv[1]=='alert':
                alert()

