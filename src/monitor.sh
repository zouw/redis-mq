#/bin/bash

LOG_PATH=/data/server_log/logs/x2_mq/monitor.log
BIN_PATH=/opt/mq/src

if [ $# -eq 1 ]
then
	if [ $1 == "setup" ]
	then
		self="${BIN_PATH}/monitor.sh"
		crontab -l > /tmp/crontab.bak
		echo "*/1 * * * * $self" >> /tmp/crontab.bak
		crontab /tmp/crontab.bak
		exit
	fi
	if [ $1 == "alert" ]
	then
		self="${BIN_PATH}/monitor.sh"
		crontab -l > /tmp/crontab.bak
		echo "*/1 * * * * /usr/bin/python $BIN_PATH/notify.py alert" >> /tmp/crontab.bak
		crontab /tmp/crontab.bak
		exit
	fi
else
	echo "setup crontab(mq path: /opt/mq/src): ./monitor.sh setup"
fi

# MQ safe restart script 
RESTART_BIN=${BIN_PATH}/restart.sh

# MQ启动后记录的进程ID列表
src_pid=($(awk '{print $0}' ${LOG_PATH}/pid))
# 实际系统运行的MQ进程列表
dst_pid=(`ps -ef | grep "python Message" |grep -v grep| awk '{print $2}'`)

#---------------------------------------------------------
# 1. 进程数比较
src_pid_len=${#src_pid[@]}
dst_pid_len=${#dst_pid[@]}

if [ ${src_pid_len} -gt ${dst_pid_len} ]
then
	nohup ${RESTART_BIN} &
	echo ""
	echo "======================Restart==============================">>${LOG_PATH}/monitor.log
	echo "==========`date`=============">>${LOG_PATH}/monitor.log
	echo "Reason: MQ process is ${dst_pid_len} (expect ${src_pid_len})">>${LOG_PATH}/monitor.log
	exit 
fi
#---------------------------------------------------------



#---------------------------------------------------------
# 进程ID比较，记录在pid的进程必须存在，否则出现异常，重启服务
for src in ${src_pid[@]}
do
	flag=0
	for dst in ${dst_pid[@]}
	do
		if [ 9$dst -eq 9$src ]
		then
			flag=1
		fi
	done
	if [ $flag -eq 0 ]
	then
		echo "======================Restart==============================">>${LOG_PATH}/monitor.log
		echo "==========`date`=============">>${LOG_PATH}/monitor.log
		echo "Reason: some MQ process not exist)">>${LOG_PATH}/monitor.log
		nohup ${RESTART_BIN} &
		exit
	fi
done
#---------------------------------------------------------


#---------------------------------------------------------
close_wait=`netstat -nal|grep :30002 |grep CLOSE_WAIT`
if [ ${close_wait} -gt 500 ]
then
	echo "======================Restart==============================">>${LOG_PATH}/monitor.log
	echo "==========`date`=============">>${LOG_PATH}/monitor.log
	echo "Reason: close_wait(30002) is ${close_wait}">>${LOG_PATH}/monitor.log
	nohup ${RESTART_BIN} &
fi
#---------------------------------------------------------

echo "Server is running correctly">>${LOG_PATH}/monitor.log
