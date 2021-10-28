import psutil
import logging
import signal

logger = logging.getLogger("util")


async def kill_all_tasks(other_task=None):
    logger.info("Killing tasks")
    dont_cancel = [asyncio.current_task(),other_task]
    tasks = [t for t in asyncio.all_tasks() if t not in dont_cancel]

    [task.cancel() for task in tasks]

    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Cancelled")



def check_memory(MEMORY_THRESHOLD=90):
    """
    We seem to have a memory leak. Seems like Pyppeteer is the prime suspect.
    Shut down gently if memory gets excessive
    """
    mem = psutil.virtual_memory()
    logger.info(f"Memory usage: {mem.percent}")
    return mem.percent > MEMORY_THRESHOLD