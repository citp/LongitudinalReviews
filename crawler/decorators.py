import asyncio
import time
import functools
import traceback
import logging
import sys


TASK_COMPLETE = 1
RETRY_INCREMENT_COUNTER = 2
RETRY_RESET_COUNTER = 3

logger = logging.getLogger("decorators")
logger.setLevel(logging.DEBUG)


def timeout(t):
    """
    Timeout after t seconds. For async functions. Wrapper for asyncio.waitfor
    """
    def wrapper(func):
        @functools.wraps(func)
        async def inner(*args,**kwargs):
            try:
                res = await asyncio.wait_for(func(*args,**kwargs), t)
            except:
                logger.debug("Failed to complete function %s" % (func.__name__))
                raise
                return None, sys.exc_info()
            logger.debug("Successfully completed function %s" % (func.__name__))
            return res
        return inner
    return wrapper


def sleep_and_retry_async(calls=5,period=5):
    """
    Calls -- Maximum number of calls
    Period -- time to sleep between

    For async functions
    """
    def inner(func):
        @functools.wraps(func)
        async def wrapper(*args, calls=calls,period=period, **kwargs):
            i = 0
            while i < calls:
                kwargs["attempts"] = i
                
                i += 1
                
                logger.debug("enter: %s (async)" % func.__name__)
                ret = await func(*args, **kwargs)
                logger.debug("exit: %s (async)" % func.__name__)

                if ret == RETRY_RESET_COUNTER:
                    logger.debug("Resetting counter")
                    i = 0
                
                if ret == RETRY_INCREMENT_COUNTER or ret == RETRY_RESET_COUNTER:
                    logger.debug("Sleeping")
                    await asyncio.sleep(period)
                elif ret == TASK_COMPLETE:
                    logger.debug("Success")
                    return True
                else:
                    raise Exception(f"Unsupported return value {ret}")
                
            raise Exception("Exceeded max attempts")
        return wrapper
    return inner

def sleep_and_retry(calls=5,period=5):
    """
    Calls -- Maximum number of calls
    Period -- time to sleep between

    For non-async functions
    """
    def inner(func):
        @functools.wraps(func)
        def wrapper(*args,  calls=calls,period=period, **kwargs):

            i = 0
            while i < calls:
                kwargs["attempts"] = i

                i += 1
                
                logger.debug("enter: %s" % func.__name__)
                ret = func(*args, **kwargs)
                logger.debug("exit: %s" % func.__name__)

                
                if ret == RETRY_RESET_COUNTER:
                    i = 0
                
                if ret == RETRY_INCREMENT_COUNTER or ret == RETRY_RESET_COUNTER:
                    logger.debug("Sleeping")
                    time.sleep(period)
                elif ret == TASK_COMPLETE:
                    return ret
                else:
                    raise Exception("Unsupported return value")
                
            raise Exception("Exceeded max attempts")
        return wrapper
    return inner
