#!/bin/bash
ps aux|grep MessageProxy | grep -v grep | awk '{print $2}'|xargs -i kill -s SIGUSR1  {}
echo -e "\n Begin safe restart "  `date`  "..."
for (( i=0; i<2; i++ )); do sleep 1; echo  "..."$i; done
ps aux|grep MessageProxy |awk '{print $2}'|xargs -i kill -9  {}
ulimit -n 102400
python  MessageProxy.py &
