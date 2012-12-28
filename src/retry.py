
import time

class RetryFailedError(Exception):
    """ Raised when retrying an op ultimately failed."""

class RetrySleeper(object):
    """ a retry sleeper that will track its backoff and  sleep
        appropriately  when asked.
    """

    def __init__(self, max_tries = 1, init_delay=0.1, max_delay=300,
                 backoff=2, sleep_func= time.sleep):
        """ create a :class:'MqRetry' instance
        :param max_tries: how many times to retry the func.
        :param delay: Initial delay between retry attemps.
        :param backoff: Default to 2 for exponential backoff
        :param max_delay: Maximum delay in seconds. 300
        """
        self.sleep_func = sleep_func
        self.max_tries = max_tries
        self.init_delay = init_delay
        self.backoff = backoff
        self.max_delay = float(max_delay)
        self._attempts = 0
        self._cur_delay = init_delay

    def reset(self):
        """ Reset the attemp counter"""
        self._attempts = 0
        self._cur_delay = self.init_delay

    def increment(self):
        """ Increment the failed  count. """
        if self._attempts == self.max_tries:
            raise RetryFailedError("Too many retry attempts.")
        self._attempts += 1
        self.sleep_func(self._cur_delay)
        self._cur_delay = min(self._cur_delay * self.backoff, self.max_delay)

class MqRetry(object):
    """ Helper for retrying a method in the case of retry-able
    exceptions
    """
    RETRY_EXCEPTIONS = (Exception)

    def __init__(self, max_tries = 1, init_delay=0.1, max_delay=300 ):
        """ create a :class:'MqRetry' instance
        :param max_tries: how many times to retry the func.
        :param delay: Initial delay between retry attemps.
        :param max_delay: Maximum delay in seconds. 300
        """
        self.retry_sleeper = RetrySleeper(max_tries, init_delay, max_delay)
        self.retry_exceptions = self.RETRY_EXCEPTIONS

    def __call__(self, func, *args, **kwargs):
        self.retry_sleeper.reset()

        while True:
            try:
                return func(*args, **kwargs)
            except self.retry_exceptions:
                self.retry_sleeper.increment()

def retry(func):
    myretry = MqRetry(3, 0.5)
    myretry(func)


def test():
    print "test retry func "
    print time.time()
    raise Exception("test")

if __name__ == "__main__":
    retry(test)

