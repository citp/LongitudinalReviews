import pyppeteer
import asyncio
import random
import time
import subprocess
from crawler.decorators import timeout
import traceback
import logging
import signal
import sys

#Monkeypatch scroller function
import crawler.patches

#Disable while we figure out ICU
import polyglot
import polyglot.text

from crawler.connections.session import ConnectionSession

MAX_USES = 10000
MAX_ATTEMPTS = 10
MAX_CALLS = 100
TIMEOUT = 240000 #Four minutes (ms)
FN_TIMEOUT = 270 #Four and a half minutes (s)


logger = logging.getLogger("connection_manager")
logger.setLevel(logging.DEBUG)


class BasicConnectionManager():

    calls = 0
    context = None
    browser = None
    
    def __init__(self, headless=True):
        self.event_loop = asyncio.get_event_loop()
        self.headless = headless

    def reset(self):
        return self.event_loop.run_until_complete(self.__reset())

    async def __reset(self):
        logger.warning("Resetting browser")
        try:
            await self.browser.close()
        except:
            logger.error("Failed to reset browser", exc_info=True)
            #await kill_all_tasks(asyncio.current_task())
            
    def start_session(self,reset=False):
        try:
            return self.event_loop.run_until_complete(self.__start_session(reset=reset))
        except:
            logger.error("Unexpected error when starting session", exc_info=True)
            return ConnectionSession(None,self.event_loop,parent=self)

    @timeout(2*FN_TIMEOUT)
    async def __start_session(self,reset=False):
        if reset:
            await self.__reset()
        try:
            #Do we need to reconnect?
            if self.context is None:
                if self.browser is not None:
                    await self.browser.close()
                logger.info("Connecting to browser")
                self.browser = await pyppeteer.launcher.launch(args=['--no-sandbox'], autoClose=False,headless=self.headless)#,logLevel=logging.DEBUG)
                self.context = await self.browser.createIncognitoBrowserContext()


            #Start session
            page = await self.context.newPage()
            session = ConnectionSession(page,self.event_loop,parent=self)
            try:
                pass
                #await session.language_check()
            except:
                logging.warning("Initial language check failed. Ignoring", exc_info=True)
        except:
            logger.error("Failed to open page",exc_info=True)

            page = None
            session = ConnectionSession(page,self.event_loop,parent=self)
        return session

    @timeout(10)
    async def __exit(self):
        if self.browser != None:
            await self.browser.close()
    
    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        #self.event_loop.stop()
        try:
            self.event_loop.run_until_complete(self.__exit())
        except:
            logger.error("Unexpected error closing session", exc_info=True)


class VPNConnectionManager:

    uses = 0
    calls = 0
    context = None
    browser = None
    
    def __init__(self, headless=True):
        
        global vpn
        import crawler.vpn as vpn
        
        self.event_loop = asyncio.get_event_loop()
        self.headless = headless

    def reset(self):
        return self.event_loop.run_until_complete(self.__reset())

    async def __reset(self):
        logger.warning("Resetting browser")
        try:
            await self.browser.close()
        except:
            logger.error("Failed to reset browser", exc_info=True)
            #await kill_all_tasks(asyncio.current_task())
        self.browser = None
        self.uses = 0
        self.calls= 0
        self.context = None
            
    def start_session(self,reset=False):
        try:
            return self.event_loop.run_until_complete(self.__start_session(reset=reset))
        except:
            logger.error("Unexpected error when starting session", exc_info=True)
            return ConnectionSession(None,self.event_loop,parent=self)

    @timeout(2*FN_TIMEOUT)
    async def __start_session(self,reset=False):
        if reset:
            await self.__reset()
        try:
            #Do we need to reconnect?
            if self.calls > MAX_CALLS or self.uses > MAX_USES or self.context is None:
                if self.browser is not None:
                    await self.browser.close()
                await vpn.cycle_connection_openvpn()
                self.browser = await pyppeteer.launcher.launch(args=["--no-sandbox"],autoClose=False,headless=self.headless)
                self.context = await self.browser.createIncognitoBrowserContext()
                self.uses = 0
                self.calls = 0

            self.uses += 1

            #Start session
            page = await self.context.newPage()
            session = ConnectionSession(page,self.event_loop,parent=self)
            try:
                pass
                #await session.language_check()
            except:
                logging.warning("Initial language check failed. Ignoring", exc_info=True)
        except:
            logger.error("Failed to open page",exc_info=True)

            page = None
            session = ConnectionSession(page,self.event_loop,parent=self)
        return session

    @timeout(10)
    async def __exit(self):
        if self.browser != None:
            await self.browser.close()
    
    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        #self.event_loop.stop()
        try:
            self.event_loop.run_until_complete(self.__exit())
        except:
            logger.error("Unexpected error closing session", exc_info=True)

if __name__ == "__main__":
    pyppeteer.DEBUG = True
    #cm = ConnectionManager([],["wlo1"])
    #cm = ProxyConnectionManager()
    cm = BasicConnectionManager()
    with cm.start_session() as session:
        if session != None:
            content, title = session.jump_to("https://example.com")
            print(title)
            content, title = session.navigate(session,"a")
            print(content)

