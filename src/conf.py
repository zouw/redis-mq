#!/usr/bin/env python
MFILE_NUM = 10
MFILE_SIZE = 2000000000
import logging
import tsrotatingfilehandler
# output to rotating log file
handler=tsrotatingfilehandler.TSRotatingFileHandler('/data/server_log/logs/x2_mq/proxy.log', 'midnight', 1, 10)
# output to console
#handler=logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(process)d %(filename)s:%(lineno)d %(message)s'))
logging.getLogger().addHandler(handler)
#logging.getLogger().setLevel(logging.INFO)
logging.getLogger().setLevel(logging.DEBUG)
logger=logging.getLogger()
