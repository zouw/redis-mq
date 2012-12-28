import os
import time
import glob
import logging
import fcntl
import logging.handlers
class TSRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None):
        logging.handlers.TimedRotatingFileHandler.__init__(self, filename, when, interval, backupCount, encoding)
        if self.when == 'MIDNIGHT':
            self.suffix = '%Y%m%d00'
        self.fileidx = self.getFileIdx()
    def getFileIdx(self):
        t = self.rolloverAt - self.interval
        timeTuple = time.localtime(t)
        dfn = self.baseFilename + '.' + time.strftime(self.suffix, timeTuple)
        s = glob.glob(dfn + '_*')
        fileidx = 1
        if len(s) > 0:
            s.sort(TSRotatingFileHandler.cmpLogFileName)
            latest_name = s[-1]
            pos = latest_name.rfind('_')
            latest_index = int(latest_name[pos+1:])
            fileidx = latest_index+1
        return fileidx

    @staticmethod
    def cmpLogFileName(l, r):
        pos = l.rfind('_')
        lname = l[0:pos]
        lidx = l[pos+1:]
        pos = r.rfind('_')
        rname = r[0:pos]
        ridx = r[pos+1:]
        res = cmp(lname, rname)
        if res != 0:
            return res
        return cmp(int(lidx), int(ridx))

    def computeRollover(self, currentTime):
        """
        Work out the rollover time based on the specified time.
        """
        result = currentTime + self.interval
        # If we are rolling over at midnight or weekly, then the interval is already known.
        # What we need to figure out is WHEN the next interval is.  In other words,
        # if you are rolling over at midnight, then your base interval is 1 day,
        # but you want to start that one day clock at midnight, not now.  So, we
        # have to fudge the rolloverAt value in order to trigger the first rollover
        # at the right time.  After that, the regular interval will take care of
        # the rest.  Note that this code doesn't care about leap seconds. :)
        if self.when == 'MIDNIGHT' or self.when.startswith('W'):
            # This could be done with less code, but I wanted it to be clear
            if self.utc:
                t = time.gmtime(currentTime)
            else:
                t = time.localtime(currentTime)
            currentHour = t[3]
            currentMinute = t[4]
            currentSecond = t[5]
            # r is the number of seconds left between now and midnight
            r = logging.handlers._MIDNIGHT - ((currentHour * 60 + currentMinute) * 60 +
                    currentSecond)
            result = currentTime + r
            # If we are rolling over on a certain day, add in the number of days until
            # the next rollover, but offset by 1 since we just calculated the time
            # until the next day starts.  There are three cases:
            # Case 1) The day to rollover is today; in this case, do nothing
            # Case 2) The day to rollover is further in the interval (i.e., today is
            #         day 2 (Wednesday) and rollover is on day 6 (Sunday).  Days to
            #         next rollover is simply 6 - 2 - 1, or 3.
            # Case 3) The day to rollover is behind us in the interval (i.e., today
            #         is day 5 (Saturday) and rollover is on day 3 (Thursday).
            #         Days to rollover is 6 - 5 + 3, or 4.  In this case, it's the
            #         number of days left in the current week (1) plus the number
            #         of days in the next week until the rollover day (3).
            # The calculations described in 2) and 3) above need to have a day added.
            # This is because the above time calculation takes us to midnight on this
            # day, i.e. the start of the next day.
            if self.when.startswith('W'):
                day = t[6] # 0 is Monday
                if day != self.dayOfWeek:
                    if day < self.dayOfWeek:
                        daysToWait = self.dayOfWeek - day
                    else:
                        daysToWait = 6 - day + self.dayOfWeek + 1
                    newRolloverAt = result + (daysToWait * (60 * 60 * 24))
                    if not self.utc:
                        dstNow = t[-1]
                        dstAtRollover = time.localtime(newRolloverAt)[-1]
                        if dstNow != dstAtRollover:
                            if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                                newRolloverAt = newRolloverAt - 3600
                            else:           # DST bows out before next rollover, so we need to add an hour
                                newRolloverAt = newRolloverAt + 3600
                    result = newRolloverAt
        return result

    def shouldRollover(self, record):
        if (self.stream and (self.stream.closed or self.stream.tell() > 2*1024*1024*1024)) or int(time.time()) >= self.rolloverAt:
            return 1
        else:
            return 0
    def doRollover(self):
        self.stream.close()
        t = self.rolloverAt - self.interval
        timeTuple = time.localtime(t)
        dfn = self.baseFilename + '.' + time.strftime(self.suffix, timeTuple)
        if int(time.time() < self.rolloverAt) :
            dfn += '_' + str(self.fileidx)
        else:
            currentTime = int(time.time())
            newRolloverAt = self.computeRollover(currentTime)
            while newRolloverAt <= currentTime:
                newRolloverAt = newRolloverAt + self.interval
            #If DST changes and midnight or weekly rollover, adjust for this.
            if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
                dstNow = time.localtime(currentTime)[-1]
                dstAtRollover = time.localtime(newRolloverAt)[-1]
                if dstNow != dstAtRollover:
                    if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                        newRolloverAt = newRolloverAt - 3600
                    else:           # DST bows out before next rollover, so we need to add an hour
                        newRolloverAt = newRolloverAt + 3600
            self.rolloverAt = newRolloverAt

        filelock = open(os.path.dirname(self.baseFilename)+'/lock', 'w')
        fcntl.lockf(filelock.fileno(), fcntl.LOCK_EX)
        s = glob.glob(self.baseFilename + '.20*')
        s.sort(TSRotatingFileHandler.cmpLogFileName)
        if not os.path.exists(dfn) and (len(s) == 0 or TSRotatingFileHandler.cmpLogFileName(dfn, s[-1]) > 0):
            os.rename(self.baseFilename, dfn)
        self.fileidx = self.getFileIdx()
        if self.backupCount > 0:
            s = glob.glob(self.baseFilename + '.20*')
            if len(s) > self.backupCount:
                s.sort(TSRotatingFileHandler.cmpLogFileName)
                os.remove(s[0])
        fcntl.lockf(filelock.fileno(), fcntl.LOCK_UN)
        filelock.close()

        if self.encoding:
            self.stream = codecs.open(self.baseFilename, 'a', self.encoding)
        else:
            self.stream = open(self.baseFilename, 'a')
if __name__=="__main__":
    handler=TSRotatingFileHandler('test.log', 'midnight', 1, 10)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(process)d %(filename)s:%(lineno)d func=%(funcName)s %(message)s'))
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    i=0
    while i < 100000000:
        logging.debug("test"*10000)
        i += 1

