from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
import logging
import math
import os
import re
import time
import traceback
import random
import signal
import sys
import argparse
import asyncio

from lxml import etree
import safer
from crawler.find_business import get_zipcodes
from tqdm import tqdm

import crawler.remote_control as rc
from crawler.util import check_memory
import crawler.io
from crawler.reviews_parser import NEXT_PAGE, NOT_RECOMMENDED_NEXT_PAGE, REMOVED_NEXT_PAGE, extract_business_data, extract_recommended, extract_not_recommended, extract_removed, sel_unusual_activity, close_unusual_activity

import crawler.connections

import ratelimit
from crawler.decorators import sleep_and_retry
import crawler.decorators as retry



#Constants
MAX_CRAWL_ATTEMPTS = 10

RECOMMENDED_REVIEWS_PER_PAGE = 10
NOT_RECOMMENDED_REVIEWS_PER_PAGE = 10
REMOVED_REVIEWS_PER_PAGE = 10

SEED = 0

def get_connection_manager():
    #return crawler.connections.ProxyConnectionManager(headless=False)
    if USE_VPN:
        return crawler.connections.VPNConnectionManager()
    else:
        return crawler.connections.BasicConnectionManager()

#Set to True to kill the program
killswitch = False
def kill_program(arg=None):
    global killswitch
    logger.warning("Setting killswitch to True")
    killswitch = True
    rc.quitting(crawler_name=CRAWLER_NAME,business_progress=businesses_crawled)
    return arg


#Singletons
#logger
logger = logging.getLogger("yelpcrawler")

@sleep_and_retry(calls=MAX_CRAWL_ATTEMPTS,period=10)
def crawl_reviews(business_id=None, business_url=None, business_data=None, recommended_reviews=None, not_recommended_reviews=None, removed_reviews=None, connection_manager=None, past_state_cache = {}, attempts=0, zipcode=None):
    #Do we need to reset the connection?
    reset = (attempts > 0)

    #Check if the program has been cancelled
    if killswitch:
        logger.warning("Killswitch set")
        sys.exit(0)
    
    logger.debug("Attempt %d to crawl %s" % (attempts+1, business_url))

    #Identifiers for the cache
    not_recommended_business_url = business_url.replace("/biz/", "/not_recommended_reviews/")
    removed_business_url = f"{data_folder}/raw_html/{business_id}/removed/"
    
    #Counting starting number of reviews
    num_cached_reviews = 0
    if business_url in past_state_cache:
        num_cached_reviews += len(past_state_cache[business_url][1])
    
    if not_recommended_business_url in past_state_cache:
        num_cached_reviews += len(past_state_cache[not_recommended_business_url][1])
        
    if removed_business_url in past_state_cache:
        num_cached_reviews += len(past_state_cache[removed_business_url][1])
    
    #Fetch reviews
    with connection_manager.start_session(reset=reset) as session:
        if session == None:
            logger.warning("Failed to initialize session")
            return retry.RETRY_INCREMENT_COUNTER
        try:
            needs_navigate = False
            
            if not SKIP_BUSINESS_DATA:
                business_data[business_id] = crawl_business_data(session, business_id, business_url,past_state_cache=past_state_cache)
                
            if not SKIP_RECOMMENDED:
                recommended_reviews[business_id] = crawl_recommended(session, business_id, business_url,past_state_cache=past_state_cache)
            else:
                needs_navigate = True
            if not SKIP_NOT_RECOMMENDED:
                not_recommended_reviews[business_id] = crawl_not_recommended(session, business_id, business_url, past_state_cache=past_state_cache,needs_navigate=True)
            else:
                needs_navigate = True
            if not SKIP_REMOVED:
                removed_reviews[business_id] = crawl_removed(session, business_id, business_url, past_state_cache=past_state_cache,needs_navigate=True)
            else:
                need_navigate = True
            
            #Kinda ugly. Delete the cache for each type. Could try to clean this up I guess
            if (business_id,"business_data") in past_state_cache:
                del past_state_cache[(business_id,"business_data")]
            if business_url in past_state_cache:
                del past_state_cache[business_url]
            if not_recommended_business_url in past_state_cache:
                del past_state_cache[not_recommended_business_url]
            if removed_business_url in past_state_cache:
                del past_state_cache[removed_business_url]
        except (KeyboardInterrupt, SystemExit):
            logger.error("Canceled. Will not continue to crawl businesses.")
            raise
        except:
            if killswitch:
                raise KeyboardInterrupt

            #Check if we made progress
            num_cached_reviews_after = 0
            if business_url in past_state_cache:
                num_cached_reviews_after += len(past_state_cache[business_url][1])

            if not_recommended_business_url in past_state_cache:
                num_cached_reviews_after += len(past_state_cache[not_recommended_business_url][1])
                
            
            if removed_business_url in past_state_cache:
                num_cached_reviews_after += len(past_state_cache[removed_business_url][1])

            progressed = (num_cached_reviews_after > num_cached_reviews)
            
            if progressed:
                command = rc.check_in(crawler_name=CRAWLER_NAME,business_active_progress=num_cached_reviews_after)
                if command == rc.REMOTE_COMMAND.QUIT:
                    kill_program()
                    raise KeyboardInterrupt
            
            logger.warning("Crawling business %s failed." % business_id,exc_info=True)
            return retry.RETRY_RESET_COUNTER if progressed else retry.RETRY_INCREMENT_COUNTER

    crawler.io.compress_raw_data(zipcode=zipcode,business_id=business_id,data_folder=data_folder,crawl_id=CRAWL_ID)
        
    return retry.TASK_COMPLETE        


def close_unusual_activity_prompt(session, content, title):

    if content is None:
        return content, title
    root = etree.HTML(content)
    if len(sel_unusual_activity(root)) == 0:
        return content, title
    logger.info("Got unusual activity prompt. Closing")
    content,title = session.click_element(close_unusual_activity, is_xpath=True)
    time.sleep(30)
    logger.info("Unusual activity prompt closed")
    return content, title

def crawl_business_data(session, business_id, business_url, past_state_cache={}):
    #TODO switch to Pyppeteer
    
    html_docs_folder = f"{data_folder}/raw_html/{business_id}/data"
    Path(html_docs_folder).mkdir(parents=True, exist_ok=True)

    logger.info(f"Raw HTML folder: {html_docs_folder}")
    
    #No integrity check needed
    # try:
    #     integrity, ret, review_count = check_crawl_integrity(html_docs_folder,not_recommended=False)
    # except:
    #     logger.warning("Failed to verify integrity", exc_info=True)
    #     integrity=False
    # if integrity:
    #     logger.info("No need to recrawl -- integrity confirmed")
    #     return ret
    
    #Remove any existing get parameters
    stripped_business_url = urljoin(business_url, urlparse(business_url).path)
    english_business_url = "%s" % stripped_business_url

    logger.info("Crawling business URL %s" % stripped_business_url)
    
    #Do we already have it?
    if (business_id,"business_data") in past_state_cache:
        return past_state_cache[(business_id,"business_data")]
    
    

    #We need to navigate to the first page to collect the data
    content, title = session.jump_to(stripped_business_url)

    if content is not None:
        content, title = close_unusual_activity_prompt(session, content, title)

    if content is None:
        logger.error("Failed to navigate to business page")
        if type(title) is tuple:
            title = title[1]
        raise title
    logger.debug("Navigated to business page")
    
    document = content

    fn = "%s/business_page.html" % (html_docs_folder)
    with safer.open(fn,"w+",temp_file=True) as f:
        f.write(document)

    #Parse page
    try:
        business_data = extract_business_data(document)
    except:
        fn = "logs/parse_fail_%s.txt" % time.strftime("%Y%m%d-%H%M")
        logger.error("Failure. Unable to parse document. Saving document to %s" % fn, exc_info=True)
        with safer.open(fn, "w+",temp_file=True) as f:
            f.write("URL: %s\nBusiness data\n%s" % (stripped_business_url, content))
        raise
    
    #Add to cache
    past_state_cache[(business_id,"business_data")] = business_data
    
    return business_data


def crawl_recommended(session, business_id, business_url, past_state_cache={}):
    #TODO switch to Pyppeteer
    
    html_docs_folder = f"{data_folder}/raw_html/{business_id}/recommended"
    Path(html_docs_folder).mkdir(parents=True, exist_ok=True)

    logger.info(f"Raw HTML folder: {html_docs_folder}")
    
    try:
        integrity, ret, review_count = check_crawl_integrity(html_docs_folder,not_recommended=False)
    except:
        logger.warning("Failed to verify integrity", exc_info=True)
        integrity=False
    if integrity:
        logger.info("No need to recrawl -- integrity confirmed")
        return ret
    
    #Remove any existing get parameters
    stripped_business_url = urljoin(business_url, urlparse(business_url).path)
    english_business_url = "%s" % stripped_business_url

    logger.info("Crawling business URL %s" % stripped_business_url)
    
    #Assume there's at least one review
    review_count = None

    #Accumulate reviews

    #Try to recover from past failure:
    if business_url in past_state_cache:
        offset, reviews = past_state_cache[business_url]
        logger.info("Had past state at offset %d with %d reviews. Resuming." % (offset, len(reviews)))
        if offset == -1:

            #We need to navigate to the first page anyways because the not_recommended reviews requires it
            content, title = session.jump_to(stripped_business_url)

            if content is not None:
                content, title = close_unusual_activity_prompt(session, content, title)

            if content is None:
                logger.error("Failed to navigate to first recommended page (1)")
                if type(title) is tuple:
                    title = title[1]
                raise title
            logger.debug("Navigated to first recommended page (1)")
            
            return reviews
    else:
        offset = 0
        reviews = []

    has_next = True
    
    while has_next: #and (review_count is None or offset < review_count):
        #Check if program was cancelled
        if killswitch:
            sys.exit(0)
        
        #Fetch page
        page = (offset / RECOMMENDED_REVIEWS_PER_PAGE + 1)

        #Calculate the URL for the page -- may not need it if we can navigate there through the UI
        url = english_business_url
        if offset != 0:
            url = "%s?start=%d" % (url,offset)
            
        if review_count is None:
            #We don't have any reviews, or we have old reviews, so we need to jump straight there
            content, title = session.jump_to(url)

            if content is not None:
                content, title = close_unusual_activity_prompt(session, content, title)
            
            if content is None:
                logger.error("Failed to navigate to first recommended page (%d)" % page)
                if type(title) is tuple:
                    title = title[1]
                raise title
            logger.debug("Navigated to first recommended page (%d)" % page)
        else:
            try:
                SLEEP_TIME = 5
                #Sleep around 5 seconds, gaussian distributed
                time.sleep(random.random() * SLEEP_TIME + random.random() * SLEEP_TIME)
                logger.info("Navigating to page %d" % page)

                method_a = lambda: session.navigate(NEXT_PAGE)
                method_b = lambda: session.jump_to(url)
                methods = [method_a,method_b]
                if random.random() > 0.05:
                    method_choice = [0,1]
                else:
                    method_choice = [1,0]

                logger.info("Trying %s first" % ("Navigate" if method_choice[0] == 0 else "URL"))
                
                try:
                    #Try to use the UI to navigate
                    content, title = methods[method_choice[0]]()
                except KeyboardInterrupt:
                    raise

                if content is None:
                    logger.info("Failed, trying %s" % ("Navigate" if method_choice[1] == 0 else "URL"))
                    #If the UI fails, go to the needed page
                    content, title = methods[method_choice[1]]()
                
            except KeyboardInterrupt:
                raise
            except BaseException as e:
                content = None
                title = e

            
            if content is not None:
                content, title = close_unusual_activity_prompt(session, content, title)
            
            if content is None:
                logger.error("Error match: %s: %s" % (str(title), "No node found for selector" in str(title)))
                logger.error("Failed to navigate to recommended page (%d)" % page,exc_info=title)
                if "No node found for selector" in str(title):
                    logger.warning("Unable to find more recommended reviews. " +
                                   "Assuming no more recommended reviews and skipping")
                    break
                past_state_cache[business_url] = (offset, reviews)
                if type(title) is tuple:
                    title = title[1]
                raise title
            logger.debug("Navigated to next recommended page  (%d)" % page)
        
        document = content

        
        fn = "%s/page_%000d.html" % (html_docs_folder, page)
        with safer.open(fn,"w+",temp_file=True) as f:
            f.write(document)

        #Parse page
        try:
            extracted = extract_recommended(document)
            actual_page = extracted["page_number"]
            assert actual_page == page, f"Mismatched page number: tried to crawl {page}, found {actual_page}"
            new_reviews = extracted["reviews"]
            new_review_count = extracted["review_count"]
            has_next = extracted["has_next"]
            logger.debug("Has next page: %s" % str(has_next))
        except:
            fn = "logs/parse_fail_%s.txt" % time.strftime("%Y%m%d-%H%M")
            logger.error("Failure. Unable to parse document. Saving document to %s" % fn, exc_info=True)
            with safer.open(fn, "w+",temp_file=True) as f:
                f.write("URL: %s\nOffset: %d\n%s" % (stripped_business_url, offset, content))
            past_state_cache[business_url] = (offset, reviews)
            raise

        if new_review_count != review_count:
            logger.info("Need to fetch %d reviews" % new_review_count)
        
        #Update data
        review_count = new_review_count
        reviews += new_reviews

        #Update offset
        offset += RECOMMENDED_REVIEWS_PER_PAGE

        #Exit if we should only crawl one page
        if FIRST_PAGE_ONLY:
            break

    try:
        assert review_count == len(reviews)
    except:
        #Looks like this does actually happen for some reason.
        #Let's flag them so we can manually inspect later.
        logger.warning("For url: %s failed review count check. Expected: %d; Found %d."
                       % (stripped_business_url, review_count, len(reviews)))
    past_state_cache[business_url] = (-1, reviews)
    return reviews


def check_crawl_integrity(html_docs_folder, not_recommended=False):
    reviews = []
    review_count = -1

    max_page = 0
    expected_pages = 0

    logger.info("Checking not recommended crawl integrity")

    #Check all cached pages
    page_files = list(os.listdir(html_docs_folder))
    page_count = 0
    
    for page_fn in tqdm(page_files,total=len(page_files)):
        if not page_fn.startswith("page_"):
            logger.debug(f"Unexpected file {page_fn}")
            continue
        page_count += 1
        
        #Get expected page number
        page_num = int(page_fn.split("_")[-1][:-5])

        max_page = max(max_page, page_num)

        #Extract the data
        with(open(os.path.join(html_docs_folder,page_fn))) as f:
            content = f.read()
        if not_recommended:
            extracted = extract_not_recommended(content)
        else:
            extracted = extract_recommended(content)

        #Page number should be as expected
        extracted_pn = extracted["page_number"]
        if extracted_pn != page_num:
            logger.info(f"Page {page_num} inconsistent -- saved as page {extracted_pn}")
            return False, None, None

        expected_pages = max(extracted["page_count"],expected_pages)

        #Save results
        review_count = extracted["review_count"]
        reviews += extracted["reviews"]

    #If we don't have any reviews, recrawl
    if review_count <= 0:
        logger.info("No reviews cached, need to recrawl")
        return False, None, None

    
    if max_page > page_count:
        logger.warning(f"Uh oh, somehow our max page is larger than expected! max page: {max_page} number of pages: {page_count}")

    if expected_pages != page_count:
        logger.info(f"Inconsistent page count -- we expected {expected_pages} pages, but found {page_count} pages, max page: {max_page}")
        return False, None, None

    logger.info(f"Cached review integrity confirmed over {page_count} pages")
    return True, reviews, review_count
    

def crawl_not_recommended(session, business_id, business_url, past_state_cache={}, needs_navigate=False):
    """
    Session should already be on the page with recommended reviews
    """

    stripped_business_url = business_url.split("?")[0]
    not_recommended_business_url = stripped_business_url.replace("/biz/", "/not_recommended_reviews/")

    #Setup folders
    html_docs_folder = f"{data_folder}/raw_html/{business_id}/not_recommended"
    Path(html_docs_folder).mkdir(parents=True, exist_ok=True)
    
    logger.debug("Crawling not recommended reviews")
    logger.debug(f"Raw HTML stored in {html_docs_folder}")

    try:
        integrity, ret, review_count = check_crawl_integrity(html_docs_folder,not_recommended=True)
    except:
        logger.warning("Failed to verify integrity", exc_info=True)
        integrity=False
    if integrity:
        logger.info("No need to recrawl -- integrity confirmed")
        return ret

    if needs_navigate:
        content, title = close_unusual_activity_prompt(session, *session.jump_to(stripped_business_url))
        if content is None:
            logger.warning("Unable to scan for unusual activity prompt, presuming failure")
            raise title[1]

    
    #Assume there's at least one review
    review_count = None
    
    #Try to recover from past failure:
    if not_recommended_business_url in past_state_cache:
        offset, reviews = past_state_cache[not_recommended_business_url]
        logger.info("Had past state at offset %d with %d reviews. Resuming." % (offset, len(reviews)))
    else:
        offset = 0
        reviews = []

    #Accumulate reviews
    while review_count is None or offset < review_count:
        #Check if program was cancelled
        if killswitch:
            sys.exit(0)
        
        #Fetch the needed page
        page = (offset // NOT_RECOMMENDED_REVIEWS_PER_PAGE + 1)
        if review_count is None:
            logger.info("First step, need to navigate to not recommended reviews")
            
            if offset > 0:
                #At an offset, jump to offset
                url = f"{not_recommended_business_url}?not_recommended_start={offset // 10 * 10}"
                logger.info(f"Navigating directly to {url}")
                content, title = session.jump_to(url)
                pass
            else:
                #Navigate to not recommended review
                try:
                    logger.info("Navigating by clicking")
                    content, title = session.navigate("a[href^='/not_recommended_reviews/']",is_xpath=False)    
                except Exception as e:
                    if "No node found for selector" in str(e):
                        logger.warning("Unable to navigate to not recommended reviews. " +
                                       "Assuming no not recommended reviews and skipping")
                        return []
                    raise e

            #Handle failures
            if content is None:
                logger.error("Failed to navigate to first not recommended page (%d)" % page,exc_info=title)
                if type(title) is tuple:
                    title = title[1]
                if "No node found for selector" in str(title):
                    logger.warning("Unable to navigate to not recommended reviews. " +
                                   "Assuming no not recommended reviews and skipping")
                    return []
                raise title
            logger.debug("Navigated to first not recommended page (%d)" % page)
        else:
            content, title = session.navigate(NOT_RECOMMENDED_NEXT_PAGE)
            if content is None:
                logger.error("Failed to navigate to next not recommended page (%d)" % page,exc_info=title)
                past_state_cache[not_recommended_business_url] = (offset, reviews)
                if type(title) is tuple:
                    title = title[1]
                raise title
            logger.debug("Navigated to next not recommended page  (%d)" % page)
        
        document = content

        fn = "%s/page_%000d.html" % (html_docs_folder, page)
        with safer.open(fn,"w+",temp_file=True) as f:
            f.write(document)
        logger.debug(f"Wrote {fn}")
        
        #Parse page
        try:
            extracted_content = extract_not_recommended(document)
        except:
            fn = "logs/parse_fail_%s.txt" % time.strftime("%Y%m%d-%H%M")
            logger.error("Failure. Unable to parse document. Saving document to %s" % fn)
            with safer.open(fn, "w+",temp_file=True) as f:
                f.write("URL: %s\nOffset: %d\n%s" % (not_recommended_business_url, offset, content))
            past_state_cache[not_recommended_business_url] = (offset, reviews)
            raise

        
        if extracted_content["page_number"] != page:
            raise Exception(f"Page number mismatch. Found {extracted_content['page_number']} expected {page}")

        new_reviews = extracted_content["reviews"]
        new_review_count = extracted_content["review_count"]


        if new_review_count != review_count:
            logger.info("Need to fetch %d reviews" % new_review_count)
        
        #Update data
        review_count = new_review_count
        reviews += new_reviews

        #Update offset
        offset += NOT_RECOMMENDED_REVIEWS_PER_PAGE #10 not 20 for not recommended

        
        #Exit if we should only crawl one page
        if FIRST_PAGE_ONLY:
            break

    try:
        assert review_count == len(reviews)
    except:
        #Looks like this does actually happen for some reason.
        #Let's flag them so we can manually inspect later.
        logger.warning("Failed review count check. Expected: %d; Found %d." % (review_count, len(reviews)))
    return reviews

def crawl_removed(session, business_id, business_url, past_state_cache={}, needs_navigate=False):
    """
    Session should already be on the page with not recommended reviews
    """

    stripped_business_url = business_url.split("?")[0]
    not_recommended_business_url = stripped_business_url.replace("/biz/", "/not_recommended_reviews/")

    #Setup folders
    html_docs_folder = f"{data_folder}/raw_html/{business_id}/removed/"
    Path(html_docs_folder).mkdir(parents=True, exist_ok=True)
    
    logger.debug("Crawling removed reviews")
    logger.debug(f"Raw HTML stored in {html_docs_folder}")

    try:
        integrity, ret, review_count = check_crawl_integrity(html_docs_folder,not_recommended=True)
    except:
        logger.warning("Failed to verify integrity", exc_info=True)
        integrity=False
    if integrity:
        logger.info("No need to recrawl -- integrity confirmed")
        return ret

    if needs_navigate:
        content, title = close_unusual_activity_prompt(session, *session.jump_to(stripped_business_url))
        if content is None:
            logger.warning("Unable to scan for unusual activity prompt, presuming failure")
            raise title[1]

    
    #Assume there's at least one review
    review_count = None
    
    #Try to recover from past failure:
    if html_docs_folder in past_state_cache:
        offset, reviews = past_state_cache[html_docs_folder]
        logger.info("Had past state at offset %d with %d reviews. Resuming." % (offset, len(reviews)))
    else:
        offset = 0
        reviews = []

    #Accumulate reviews
    while review_count is None or offset < review_count:
        #Check if program was cancelled
        if killswitch:
            sys.exit(0)
        
        
        #FIXME navigation logic
        #Fetch the needed page
        page = (offset // NOT_RECOMMENDED_REVIEWS_PER_PAGE + 1)
        if review_count is None:
            logger.info("First step, need to navigate to removed reviews")
            
            if offset > 0:
                #At an offset, jump to offset
                url = f"{not_recommended_business_url}?removed_start={offset // 10 * 10}"
                logger.info(f"Navigating directly to {url}")
                content, title = session.jump_to(url)
                pass
            else:
                #Navigate to not recommended review
                try:
                    #logger.info("No navigation needed")
                    session.jump_to(f"{stripped_business_url}")
                    logger.info("Navigating by clicking")
                    content, title = session.navigate("a[href^='/not_recommended_reviews/']",is_xpath=False)    
                except Exception as e:
                    if "No node found for selector" in str(e):
                        logger.warning("Unable to navigate to removed reviews. " +
                                       "Assuming no removed reviews and skipping")
                        return []
                    raise e

            #Handle failures
            if content is None:
                logger.error("Failed to navigate to first removed page (%d)" % page,exc_info=title)
                if type(title) is tuple:
                    title = title[1]
                if "No node found for selector" in str(title):
                    logger.warning("Unable to navigate to removed reviews. " +
                                   "Assuming no removed reviews and skipping")
                    return []
                raise title
            logger.debug("Navigated to first removed page (%d)" % page)
        else:
            content, title = session.navigate(REMOVED_NEXT_PAGE)
            if content is None:
                logger.error("Failed to navigate to next removed page (%d)" % page,exc_info=title)
                past_state_cache[html_docs_folder] = (offset, reviews)
                if type(title) is tuple:
                    title = title[1]
                raise title
            logger.debug("Navigated to next removed page  (%d)" % page)
        
        document = content

        fn = "%s/page_%000d.html" % (html_docs_folder, page)
        with safer.open(fn,"w+",temp_file=True) as f:
            f.write(document)
        logger.debug(f"Wrote {fn}")
        
        #Parse page
        try:
            extracted_content = extract_removed(document)
        except:
            fn = "logs/parse_fail_%s.txt" % time.strftime("%Y%m%d-%H%M")
            logger.error("Failure. Unable to parse document. Saving document to %s" % fn)
            with safer.open(fn, "w+",temp_file=True) as f:
                f.write("URL: %s\nOffset: %d\n%s" % (not_recommended_business_url, offset, content))
            past_state_cache[html_docs_folder] = (offset, reviews)
            raise

        
        if extracted_content["page_number"] != page:
            raise Exception(f"Page number mismatch. Found {extracted_content['page_number']} expected {page}")

        new_reviews = extracted_content["reviews"]
        new_review_count = extracted_content["review_count"]


        if new_review_count != review_count:
            logger.info("Need to fetch %d reviews" % new_review_count)
        
        #Update data
        review_count = new_review_count
        reviews += new_reviews

        #Update offset
        offset += REMOVED_REVIEWS_PER_PAGE #10 not 20 for not recommended

        
        #Exit if we should only crawl one page
        if FIRST_PAGE_ONLY:
            break

    try:
        assert review_count == len(reviews)
    except:
        #Looks like this does actually happen for some reason.
        #Let's flag them so we can manually inspect later.
        logger.warning("Failed review count check. Expected: %d; Found %d." % (review_count, len(reviews)))
    return reviews
        
def main_zipcode(zipcodes=None):
    global num_businesses, businesses_crawled

    num_businesses = count_businesses_in_zips(zipcodes)
    businesses_crawled = 0
    logger.info(f"Identified {num_businesses} businesses for crawling")

    rc.initialize(crawler_name=CRAWLER_NAME,zip_count=len(zipcodes),business_count=num_businesses)
    
    for i,zipcode in enumerate(zipcodes):
        logger.info(f"Zipcode ({i}/{len(zipcodes)}): {zipcode}")
        count_crawled_for_zip(zipcode)
    
    #Go through each zipcode and crawl
    with get_connection_manager() as cm:
        for i,zipcode in enumerate(zipcodes):
            logger.info(f"Zipcode ({i}/{len(zipcodes)}): {zipcode}")
            command = rc.check_in(crawler_name=CRAWLER_NAME,zip_progress=i,zip_active_zip=zipcode)
            if command == rc.REMOTE_COMMAND.QUIT:
                logger.info("Remote requested early quit")
                kill_program()
                return
            crawl_zip(zipcode, cm)
            if check_memory() or killswitch:
                logger.info("Quitting early")
                return

    logger.info("Finished crawling")


def count_businesses_in_zips(zipcodes):
    return sum([len(list(crawler.io.get_zipcode_businesses(zipcode))) for zipcode in zipcodes])


def count_crawled_for_zip(zipcode):

    global businesses_crawled
    
    #Iterate reviews
    logger.debug(f"Counting crawled businesses for zipcode {zipcode}")
    business_urls_and_ids = list(crawler.io.get_zipcode_businesses(zipcode))

    reviews, business_data, not_recommended_reviews, removed_reviews = crawler.io.get_crawled_reviews_for_zipcode(zipcode=zipcode,data_folder=data_folder)
    
    for i,(business_url,business_id) in enumerate(business_urls_and_ids):

        if business_id in reviews and business_id in not_recommended_reviews and business_id in removed_reviews:
            businesses_crawled += 1


def crawl_zip(zipcode, cm):

    global businesses_crawled
    
    #Iterate reviews in zipcode
    logger.debug(f"Crawling reviews for zipcode {zipcode}")
    business_urls_and_ids = list(crawler.io.get_zipcode_businesses(zipcode))
    
    rc.check_in(crawler_name=CRAWLER_NAME,
                zip_active_progress=0,
                zip_active_count=len(business_urls_and_ids))

    rand = random.Random(SEED+1)
    rand.shuffle(business_urls_and_ids)

    #Check if we already have reviews
    business_data, reviews, not_recommended_reviews, removed_reviews = crawler.io.get_crawled_reviews_for_zipcode(zipcode=zipcode,data_folder=data_folder)

    if RECRAWL and len(reviews) == 0 and len(not_recommended_reviews) == 0:
        logger.debug("Don't have any reviews for this zipcode during recrawl")
        return

    terminate = lambda : kill_program(crawler.io.save_reviews_zipcode(zipcode=zipcode, business_data=business_data, recommended_reviews=reviews, not_recommended_reviews=not_recommended_reviews, removed_reviews=removed_reviews, data_folder=data_folder))
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGHUP, terminate)
    signal.signal(signal.SIGTERM, terminate)

    try:
        logger.debug("Found %d businesses; already have %d" %
                     (len(business_urls_and_ids), len(reviews)))
        for i,(business_url,business_id) in enumerate(business_urls_and_ids):

            logger.info(f"Considering crawl of business {i+1}/{len(business_urls_and_ids)} in zip {zipcode}: {business_id}. Overall progress: {businesses_crawled}/{num_businesses}")
            
            #Exit if memory usage is excessive
            if check_memory():
                break

            if not RECRAWL and (business_id in reviews or SKIP_RECOMMENDED) and (business_id in not_recommended_reviews or SKIP_NOT_RECOMMENDED) and (business_id in removed_reviews or SKIP_REMOVED):
                #If we already have some reviews, assume we don't need more
                logger.debug("Already have business %s ; url: %s. Skipping" %
                             (business_id, business_url))
            else:
                command = rc.check_in(crawler_name=CRAWLER_NAME,
                                      business_progress=businesses_crawled,
                                      zip_active_progress=i+1,
                                      business_active_url=business_url,
                                      business_active_progress=0)
                
                if command == rc.REMOTE_COMMAND.QUIT:
                    logger.info("Remote requested early quit")
                    raise SystemExit()
                crawl_reviews(business_id=business_id, business_url=business_url, business_data=business_data, recommended_reviews=reviews, not_recommended_reviews=not_recommended_reviews, removed_reviews=removed_reviews, connection_manager=cm, zipcode=zipcode)
                businesses_crawled += 1
    except Exception as e:
        logger.error("Crawling failed, quitting", exc_info=True)
        #Don't actually move to the next zip -- fail until things work reliably
        raise e
    finally:
        #Save reviews
        crawler.io.save_reviews_zipcode(zipcode=zipcode, business_data=business_data, recommended_reviews=reviews, not_recommended_reviews=not_recommended_reviews, removed_reviews=removed_reviews, data_folder=data_folder)

def main_mukherjee_recrawl():
    #with crawler.connections.InterfaceConnectionManager([],["tun0"]) as cm:
    with get_connection_manager() as cm:
    
        #Iterate Mukherjee reviews
        logger.debug("Crawling Mukherjee reviews")
        business_urls_and_ids = list(crawler.io.get_mukherjee_businesses())

        rand = random.Random(SEED+0)
        rand.shuffle(business_urls_and_ids)

        #Check if we already have reviews
        reviews, not_recommended_reviews = list(crawler.io.get_crawled_reviews_for_mukherjee_recrawl())

        terminate = lambda : kill_program(crawler.io.save_reviews_mukherjee_recrawl(reviews, not_recommended_reviews))
        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGHUP, terminate)
        
        try:
            logger.debug("Found %d businesses; already have %d" %
                         (len(business_urls_and_ids), len(reviews)))

            idx = 0
            
            for business_url,business_id in business_urls_and_ids:
                idx += 1
                #Exit if memory usage is excessive
                if check_memory():
                    break
                
                if not RECRAWL and business_id in reviews and business_id in not_recommended_reviews and len(not_recommended_reviews[business_id]) > 0:
                    #If we already have some reviews, assume we don't need more
                    logger.debug("Already have business %s ; url: %s. Skipping" %
                                 (business_id, business_url))
                    continue

                logger.info(f"Doing crawl of business {idx} of {len(business_urls_and_ids)}")
                crawl_reviews(business_id, business_url, reviews, not_recommended_reviews, cm, zipcode="mukherjee_chicago")
        except Exception as e:
            logger.error("Crawling failed." +
                         " Resetting connection manager", exc_info=True)
#                cm.reset()
#
#                #Don't actually move to the next zip -- fail until things work reliably
            raise e
        finally:
            #Save reviews
            crawler.io.save_reviews_mukherjee_recrawl(reviews, not_recommended_reviews)

            
async def shutdown(signal, loop):
#From https://www.roguelynn.com/words/asyncio-exception-handling/
    
    logger.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Flushing metrics")
    loop.stop()

    kill_program()
            
def handle_shutdown():
    signals = (signal.SIGINT, signal.SIGHUP, signal.SIGTERM)
    event_loop = asyncio.get_event_loop()
    for s in signals:
        event_loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, event_loop)))
            
if __name__ == "__main__":

    handle_shutdown()

    parser = argparse.ArgumentParser(description='Crawl Yelp Reviews')
    parser.add_argument('--suppress_cycle', dest='suppress_cycle', action='store_const',
                    const=True, default=False,
                    help='Prevent VPN from cycling. Will connect if not connected')
    parser.add_argument('--no_vpn', dest='use_vpn', action='store_const',
                    const=False, default=True,
                    help="Don't connect to the VPN")
    parser.add_argument('--skip_business_data', dest='skip_business_data', action='store_const',
                    const=True, default=False,
                    help='Skip business data')
    parser.add_argument('--skip_recommended', dest='skip_recommended', action='store_const',
                    const=True, default=False,
                    help='Skip recommended reviews')
    parser.add_argument('--skip_not_recommended', dest='skip_not_recommended', action='store_const',
                    const=True, default=False,
                    help='Skip not recommended reviews')
    parser.add_argument('--skip_removed', dest='skip_removed', action='store_const',
                    const=True, default=False,
                    help='Skip removed reviews')
    parser.add_argument('--first_page_only', dest='first_page_only', action='store_const',
                    const=True, default=False,
                    help='Only check first page')
    parser.add_argument('--yelp_zip', dest='yelp_zip', action='store_const',
                    const=True, default=False,
                    help='True to crawl Zipcode. False to crawl Mukherjee data')
    parser.add_argument('--chicago_zip', dest='chicago_zip', action='store_const',
                    const=True, default=False,
                    help='Crawl Chicago Zipcodes.')
    parser.add_argument('--recrawl', dest='recrawl', action='store_const',
                    const=True, default=False,
                    help='Recrawl')
    parser.add_argument('--save', dest='save', action='store_const',
                    const=True, default=False,
                    help='Force save even during a skip run')
    parser.add_argument('--crawl_id', dest='crawl_id', default=None, type=str, help="Crawl Identifier -- used for file naming")
    parser.add_argument('--crawler_name', dest='crawler_name', default="default", type=str, help="Crawler Identifier -- used for identifying to the remote server")
    parser.add_argument('--zipcode_file', dest='zip_file', default=None, type=str, help="File containing the target ZIP codes")

    parser.add_argument('--no_crawl', dest='do_crawl', default=True, const=False, action='store_const', help="Don't actually crawl. For debugging purposes")


    
    args = parser.parse_args()
    
    import crawler.vpn.openvpn
    crawler.vpn.openvpn.SUPPRESS_CYCLE = args.suppress_cycle

    CRAWL_ID = args.crawl_id
    CRAWLER_NAME = args.crawler_name
    
    USE_VPN = args.use_vpn
    
    SKIP_BUSINESS_DATA = args.skip_business_data
    SKIP_RECOMMENDED = args.skip_recommended
    SKIP_NOT_RECOMMENDED = args.skip_not_recommended
    SKIP_REMOVED = args.skip_removed
    
    FIRST_PAGE_ONLY = args.first_page_only
    RECRAWL = args.recrawl
    
    #Save if we're not skipping anything, or if requested
    SAVE = args.save or not any([FIRST_PAGE_ONLY, any([SKIP_NOT_RECOMMENDED, SKIP_RECOMMENDED, SKIP_REMOVED]) and not args.recrawl])
    
    DRY_RUN = False

    start_time = time.strftime('%Y%m%d-%H%M')
    try:
        os.mkdir(f"logs/{CRAWL_ID}")
    except:
        pass
    fileHandler = logging.FileHandler(f"logs/{CRAWL_ID}/yelp_review_crawl_{start_time}.txt")
    streamHandler = logging.StreamHandler()
    handlers = [streamHandler] if DRY_RUN else [fileHandler,streamHandler]
    
    logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                       handlers=handlers)
    logger.setLevel(logging.DEBUG)

    data_folder = "data"
    if CRAWL_ID is not None:
        data_folder = f"{data_folder}/{CRAWL_ID}"

    target_zipcodes = None
    
    CHICAGO_ZIPCODES = args.chicago_zip
    if args.zip_file:
        with open(args.zip_file) as f:
            target_zipcodes = f.read().strip().split(' ')
    else:
        target_zipcodes = list(get_zipcodes(use_chicago_zipcodes=CHICAGO_ZIPCODES))
        rand = random.Random(SEED+0)
        rand.shuffle(target_zipcodes)


    try:

        if args.do_crawl:
            if args.yelp_zip:
                main_zipcode(zipcodes=target_zipcodes)
            else:
                main_mukherjee_recrawl()
    except:
        logger.error("Unknown error:", exc_info = True)
    finally:
        try:
            businesses_crawled
        except:
            business_crawled = 0
        rc.quitting(crawler_name=CRAWLER_NAME,business_progress=businesses_crawled)
        #Make sure to kill any outstanding processes
        logger.info("Completed. Killing process")
        os.kill(os.getpid(), signal.SIGINT)
        
