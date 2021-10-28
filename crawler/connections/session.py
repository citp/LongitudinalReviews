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

import crawler.patches

#Disable while we figure out ICU
import polyglot
import polyglot.text

TIMEOUT = 60000 #One minute (ms)
FN_TIMEOUT = 270 #Four and a half minutes (s)

MAX_CALLS = 100


logger = logging.getLogger("connection_session")
logger.setLevel(logging.DEBUG)


def check_content(content):
    s1 = "Ok, ok, ok we know that’s not an excuse. We’ll have it back up-and-running for you shortly."
    s2 = "The dog ate our website"
    assert s1 not in content and s2 not in content


class ConnectionSession:
    def __init__(self,page,event_loop,parent=None):
        self.page = page
        self.event_loop = event_loop
        self.parent = parent
        
        
    async def setup(self):
        #Adapted from https://stackoverflow.com/questions/61015095/puppeteer-block-images
        await self.page.setRequestInterception(True)

        async def block_image(req):
            if req.resourceType == 'image':
                await req.abort()
            else:
                await req.continue_()
        self.page.on('request', block_image)
        
            

    @timeout(2*FN_TIMEOUT)
    async def language_check(self,do_jump=True):

        logger.info("Doing language check")

        if do_jump:
            content, error = await self.__jump_to("https://www.yelp.com",check_lang=False)
            if content is None:
                raise error[1]
        else:
            content = await self.page.content()

        language = polyglot.text.Text(content).language
        logger.info("Language: %s" % language.name)
        if language.code == "en":
            logger.info("English... returning")
            return
        
        await self.page.click("div.footer-language div.dropdown.js-dropdown.dropdown--hover")
        
        element = await self.page.xpath("//*[contains(text(),'English (United States)')]")
        await asyncio.gather(
            self.page.waitForNavigation(timeout=TIMEOUT),
            element.click()
        )

        logger.info("Successfully changed language")
        

    def jump_to(self,*args,**kwargs):
        logger.debug(f"Jump to: {args[0]}, args: {kwargs}")
        if self.parent:
            self.parent.calls += 1
        return self.event_loop.run_until_complete(self.__jump_to(*args,**kwargs))

    @timeout(FN_TIMEOUT)
    async def __jump_to(self, url, check_lang=True, waitfor=None):
        #We've gotten banned from clicking on a single page too many times before
        try:
            if self.parent.calls > MAX_CALLS:
                raise Exception("Exceeded maximum number of calls")
        except:
            return None, sys.exc_info()
        
        try:
            if waitfor is None:
                waitObj = self.page.waitForNavigation(timeout=TIMEOUT)
            else:
                waitObj = waitfor(self.page)
            results = await asyncio.gather(
                waitObj,
                self.page.goto(url,timeout=TIMEOUT), return_exceptions=True
            )

            raiseme = None
            for result in results:
                if isinstance(result,BaseException):
                    logger.error("Gathered exception: %s" % str(result))
                    if raiseme is None:
                        raiseme = result
            if raiseme is not None:
                logger.error("Failed to jump")
                #await kill_all_tasks(asyncio.current_task())
                #logger.info("Killed all tasks")
                try:
                    raise raiseme
                except:
                    return None,sys.exc_info()
        except:
            logger.error("Should never reach here jumpto")
            raise

        if check_lang:
            try:
                await self.language_check(do_jump=False)
            except:
                logger.error("Language check failed", exc_info=True)
        
        content = await self.page.content()
        title = await self.page.title()

        try:
            check_content(content)
        except:
            return None,sys.exc_info()
        
        logger.debug("Finished jumping to page")
        return content, title

    def click_element(self,*args,**kwargs):
        """
        Like navigate, but does not wait for navigation
        """
        try:
            logger.debug("Event loop is running? %s" % self.event_loop.is_running())
            return self.event_loop.run_until_complete(self.__click_element(*args,**kwargs))
        except:
            logging.error("Error clicking element", exc_info=True)
            return None,sys.exc_info()

    @timeout(FN_TIMEOUT)
    async def __click_element(self, selector, is_xpath=False):
        """
        Like navigate, but does not wait for navigation
        """

        try:
            if is_xpath:
                logger.debug("XPath selector %s" % selector)
                
                #XPath can return multiple elements
                elements = await asyncio.wait_for(self.page.xpath(selector),10)

                #Get the first element, warn/fail if there isn't exactly one
                if len(elements) == 0:
                    raise Exception("Did not find element")
                elif len(elements) > 1:
                    logger.warning("Found more than one match for click element")
                    logger.info(elements)

                element = elements[0]

                #Click the element
                logger.debug("Clicking element")

                
                
                await element.click(timeout=TIMEOUT)
            else:
                logger.debug("JQuery Selector %s" % selector)

                #Use the builtin click
                await asyncio.wait_for(self.page.click(selector),10)
        except:
            #If we time out, save the crash so we can inspect it
            fn = "logs/selector_fail_%s.txt" % time.strftime("%Y%m%d-%H%M")
            logger.error("Failure. Unable to parse document. Saving document to %s" % fn, exc_info=True)
            try:
                content = None
                content = await asyncio.wait_for(self.page.content(),10)
            finally:
                with open(fn, "w+") as f:
                    f.write("Selector: %s\n%s" % (selector, content))
            
            logger.info("Saved")
            raise
        
        #Get content and return
        try:
            content = await self.page.content()
            title = await self.page.title()
            logger.debug("Finished clicking element on page")
            
            return content, title
        except:
            logger.error("Should never reach here click element 2")
            raise

    
    def navigate(self,*args,**kwargs):
        if self.parent:
            self.parent.calls += 1
        try:
            logger.debug("Event loop is running? %s" % self.event_loop.is_running())
            return self.event_loop.run_until_complete(self.__navigate(*args,**kwargs))
        except:
            logger.error("Failed to navigate to page", exc_info=True)
            return None, sys.exc_info()

    @timeout(FN_TIMEOUT)
    async def __navigate(self, selector, is_xpath=False):
        #We've gotten banned from clicking on a single page too many times before
        try:
            if self.parent.calls > MAX_CALLS:
                raise Exception("Exceeded maximum number of calls")
        except:
            return None, sys.exc_info()


        logger.info("Trying to navigate")

        #Click the desired element, wait until the page changes
        try:
            await asyncio.gather(self.page.waitForNavigation(timeout=3*TIMEOUT),
                                 self.__click_element(selector,is_xpath=is_xpath))
        except BaseException as e:
            logger.warning("Killing tasks after failed navigation", exc_info=True)
            logger.warning("Just kidding, not killing tasks")
            #await kill_all_tasks(asyncio.current_task())
            #logger.warning("Killed all tasks")
            return None, e

        #Get the page content and return
        try:
            content = await self.page.content()
            title = await self.page.title()
            logger.debug("Finished navigating to page")
        except:
            logger.error("Should never reach here navigate 2")
            raise

        
        try:
            check_content(content)
        except:
            return None,sys.exc_info()
        
        return content, title

    def close(self):
        self.end_session()

    def end_session(self):
        logger.debug("Stopping session")
        try:
            ret = self.event_loop.run_until_complete(self.__end_session())
        except:
            ret = None
            logger.error("Failed to stop session", exc_info=True)
        logger.debug("Stopped session")
        return ret

    @timeout(10)
    async def __end_session(self):
        try:
            await self.page.close()
        except:
            logger.error("Failed to close page",exc_info=True)

    def __enter__(self):
        if self.page is None:
            return None
        else:
            self.event_loop.run_until_complete(self.setup())
            return self

    def __exit__(self,*args):
        if self.page is None:
            pass
        else:
            self.end_session()
