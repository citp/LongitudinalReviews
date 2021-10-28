import json
import random
import subprocess
import logging
from crawler.decorators import sleep_and_retry_async
import crawler.decorators as retry
import time
import asyncio
import glob
import io
import string
import atexit
import signal

USE_SSH = False

logger = logging.getLogger("vpn.openvpn")
logger.setLevel(logging.DEBUG)


#How many times should we try to reconnect to VPN?
MAX_ATTEMPTS = 30
#How long do we need to sleep between attempts?
ATTEMPT_SLEEP = 10

#If true, won't reset the VPN when requested
SUPPRESS_CYCLE = False

TIMEOUT = 240


success_msg = b"Initialization Sequence Completed"
auth_fail_msg = b"AUTH_FAILED"

openvpn_command_part_1 = ["sudo", "openvpn", "--connect-retry-max", "2", "--config"]
openvpn_command_part_2 = ["--auth-user-pass", "vpn_credentials.txt", "--dhcp-option", "DNS", "8.8.8.8", "--dhcp-option", "DNS", "8.8.4.4"]
set_google_dns = ["sudo", "systemd-resolve", "-i", "tun0", "--set-dns=8.8.8.8", "--set-dns=8.8.4.4"]
kill_openvpn = ["sudo", "killall", "openvpn"]
kill_ssh = ["sudo", "killall", "ssh"]
ovpn_files = glob.glob("ovpn_files/*.ovpn")

active_process = None
ssh_proc = None

async def killall_openvpn():
    proc = await asyncio.wait_for(asyncio.create_subprocess_exec(
        *kill_openvpn,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE), TIMEOUT)

    await proc.wait()
    

async def close_active_connection():

    global ssh_proc
    #global active_process
    
    
    logger.info("Terminating old SSH and OpenVPN sessions")
    ssh_proc.send_signal(signal.SIGINT)
    #active_process.terminate()

    stdout, stderr = await ssh_proc.communicate(b"")
    logger.info("SSH output:\n{stdout}\n{stderr}")

    ssh_proc = None
    #active_process = None
    
@sleep_and_retry_async(calls=MAX_ATTEMPTS, period=ATTEMPT_SLEEP)
async def cycle_connection(attempts=0):

    global active_process
    if USE_SSH and ssh_proc is not None:
        if SUPPRESS_CYCLE:
            return retry.TASK_COMPLETE
        await close_active_connection()
    await killall_openvpn()

    ovpn_file = random.choice(ovpn_files)
    logger.info(f"Connecting to OVPN server: {ovpn_file}")
    
    cmd = openvpn_command_part_1 + [ovpn_file] + openvpn_command_part_2
    
    proc = await asyncio.wait_for(asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT), TIMEOUT)

    #logger.info("Writing login credentials")
    #proc.stdin.writelines([username,password])
    #await asyncio.wait_for(proc.stdin.drain(), TIMEOUT)

    while True:
        line = await asyncio.wait_for(proc.stdout.readline(), TIMEOUT)
        line_txt = line.decode()
        filtered_line = []
        for c in line_txt:
            if c == "\n": continue
            if c in string.printable:
                filtered_line.append(c)
            else:
                filtered_line.append(hex(ord([0])))
                
        print("".join(filtered_line))

        if len(line) == 0:
            logger.info("Failed to connect, trying again")
            return retry.RETRY_INCREMENT_COUNTER
        
        if success_msg in line:

            await fix_dns()
            
            logger.info("Successfully connected")
            active_process = proc

            if USE_SSH:
                await open_ssh()

            if get_apparent_ip() != starting_ip:
                return retry.TASK_COMPLETE
            else:
                logger.info("IP address did not change, retrying")
                return retry.RETRY_INCREMENT_COUNTER

async def fix_dns():
    logger.info("Setting up Google DNS")
    dns_proc = await asyncio.create_subprocess_exec(*set_google_dns, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    await dns_proc.wait()

        
async def open_ssh():
    global ssh_proc
    logger.info("Killing prior SSH instances")
    await (await asyncio.create_subprocess_exec(*kill_ssh)).wait()
    logger.info("Opening SSH port")
    ssh_proc = await asyncio.create_subprocess_exec("bash", "setup_reverse_tunnel.sh", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

def get_credentials():
    with open("vpn_credentials.json") as f:
        creds = json.load(f)
        
    username = creds["username"].encode()
    password = creds["password"].encode()
    return (username, password)

def get_apparent_ip():
    import requests
    ip = requests.get("http://ipinfo.io/ip").text
    logger.info(ip)
    return ip


@atexit.register
def cleanup_openvpn():
    subprocess.run(kill_openvpn)
    
username, password = get_credentials()

starting_ip = get_apparent_ip()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    logger.info("Starting. Current IP:")
    try:
        logger.info("Trying to get IP")
        ip = get_apparent_ip()
        logger.info(f"IP: {ip}")
    except:
        import traceback
        traceback.print_exc()

    
    event_loop = asyncio.get_event_loop()
    connected = event_loop.run_until_complete(cycle_connection())
    #connected = event_loop.run_until_complete(open_ssh())
    logger.info("Connected? %s" % connected)

    logger.info("Sleeping...")
    time.sleep(10)

    try:
        logger.info("Trying to get IP")
        ip = get_apparent_ip()
        logger.info(f"IP: {ip}")
    except:
        import traceback
        traceback.print_exc()

    time.sleep(1000000)
    
    logger.info("Disconnecting")
    event_loop.run_until_complete(close_active_connection())

    logger.info("Done, exiting...")
