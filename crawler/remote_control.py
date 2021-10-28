"""
Report progress to a monitoring server
"""


import requests
import urllib.parse
import enum
import logging
import os
import psutil
import crawler.configuration as config

import controller.control as control_ws

logger = logging.getLogger("remote_control")
logger.setLevel(logging.DEBUG)

NO_RC = False

REMOTE = config.get_configuration(["controller","hostname"])
SCHEME="https"
CERTIFICATE_FILE = "certificates/cert.pem"

request_kwargs = {}

class REMOTE_COMMAND(enum.Enum):
    NOP = 0
    QUIT = 1

def make_api_request(endpoint="",data=None,method="POST", enforce=[]):

    #Check if remote control enabled
    if NO_RC:
        return {}

    
    for arg in enforce:
        if arg not in data:
            logger.warning(f"Missing argument {arg}, aborting")
            return {}

    
    data["key"] = control_ws.KEY
    url = urllib.parse.urlunparse([SCHEME,REMOTE,endpoint,None,None,None])
    try:
        req = requests.request(method, url, data=data, **request_kwargs)
    except:
        #No need to put the key in the logs
        del data["key"]
        logger.exception(f"Failed to request to {endpoint} with data {data}")
        return {}

    try:
        text = req.text
        return req.json()
    except:
        logger.warning(f"JSON parsing failed request to {endpoint} with data {data} with response {text}")
        return {}

def initialize(**kwargs):
    answer = make_api_request(endpoint=control_ws.PAGE_INITIALIZE,data=kwargs,enforce=["crawler_name", "zip_count", "business_count"])
    
    if "quit" in answer:
        return REMOTE_COMMAND.QUIT
    
    return REMOTE_COMMAND.NOP
    
#https://stackoverflow.com/questions/787776/find-free-disk-space-in-python-on-os-x/7285483#7285483
def get_system_health(data):
    """Return disk usage statistics about the given path.

    Returned valus is a named tuple with attributes 'total', 'used' and
    'free', which are the amount of total, used and free space, in bytes.
    """
    st = os.statvfs('/')
    free = st.f_bavail * st.f_frsize
    total = st.f_blocks * st.f_frsize
    used = (st.f_blocks - st.f_bfree) * st.f_frsize
    data["system_disk_total"] = f"{int(total / 10**9)}GB"
    data["system_disk_used"] = f"{int(used / 10**9)}GB"
    data["system_disk_free"] = f"{int(free / 10**9)}GB"
    data["system_disk_utilization"] = int(used / total * 100)
    
    data["system_memory_utilization"] = int(psutil.virtual_memory().percent)
    data["system_cpu_utilization"] = psutil.cpu_percent()
    
def check_in(**kwargs):
    
    get_system_health(kwargs)
    
    answer = make_api_request(endpoint=control_ws.PAGE_CHECK_IN,data=kwargs,enforce=["crawler_name"])
    
    if "quit" in answer:
        return REMOTE_COMMAND.QUIT
    
    return REMOTE_COMMAND.NOP


has_quit = False
def quitting(**kwargs):
    global has_quit
    if has_quit:
        return
    has_quit = True
    answer = make_api_request(endpoint=control_ws.PAGE_QUIT,data=kwargs,enforce=["crawler_name"])
    return REMOTE_COMMAND.NOP


if __name__ == "__main__":
    initialize(crawler_name="test",zip_count=5,business_count=10)
    check_in(crawler_name="test",zip_progress=1,business_progress=2)
    quitting(crawler_name="test",business_progress=3)
    print(make_api_request(endpoint=control_ws.PAGE_STATUS,data={"key":control_ws.KEY},method="GET"))
