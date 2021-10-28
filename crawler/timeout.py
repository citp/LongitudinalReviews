import asyncio
import functools
import logging


logger = logging.getLogger("timeout")
logger.setLevel(logging.DEBUG)


def timeout(timeout):
    def wrapper(func):
        @functools.wraps(func)
        async def inner(*args,**kwargs):
            res = await asyncio.wait_for(func(*args,**kwargs), timeout)
            logger.debug("Successfully completed function %s" % (func.__name__))
            return res
        return inner
    return wrapper
