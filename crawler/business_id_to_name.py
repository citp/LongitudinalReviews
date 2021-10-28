import crawler.yelp_api as yelp_api
import pyzipcode
import logging
import json
import random
import time
import itertools
import argparse
import traceback
import shutil
import os

import crawler.categories

#Singletons
logger = logging.getLogger("find_business")
zipcodedb = pyzipcode.ZipCodeDatabase()

#Constants
LIMIT = 50
FETCH_LIMIT = 1000
CALL_LIMIT = 3500


def fetch_data_for_id(business_id):
    return yelp_api.get_business(business_id)

def main():
    with open("data/hotelIDs.txt") as f:
        business_ids = f.readlines()

    with open("data/restaurantIDs.txt") as f:
        business_ids += f.readlines()


    #Remove trailing newlines
    business_ids = (bid.strip() for bid in business_ids)
    #Remove empty lines
    business_ids = filter(bool,business_ids)

    #Load old data if possible
    fn = "data/businessid_to_data.json"
    try:
        with open(fn) as f:
            business_data = json.load(f)
    except:
        business_data = {}
    try:
        #Get new data
        for business_id in business_ids:
            if business_id in business_data and "error" not in business_data[business_id]:
                continue
            logger.info("Collecting data for business %s" % business_id)
            if DRY_RUN:
                continue
            business_data[business_id] = fetch_data_for_id(business_id)
    finally:
        #Save data
        if not DRY_RUN:
            with open("%s.tmp" % fn, "w+") as f:
                json.dump(business_data, f)
            shutil.copy("%s.tmp" % fn, fn)
                

if __name__ == "__main__":

    
    parser = argparse.ArgumentParser(description='Get YELP businesses')
    parser.add_argument('--dry_run', dest='dry_run', action='store_const',
                    const=True, default=False,
                    help='Do a dry run')
    parser.add_argument('--call_limit', type=int, default=4500,
                    help='Max number of API calls')

    args = parser.parse_args()
    DRY_RUN = args.dry_run
    CALL_LIMIT = args.call_limit
    
    fileHandler = logging.FileHandler("logs/yelp_api_crawl_%s.txt" % time.strftime("%Y%m%d-%H%M"))
    streamHandler = logging.StreamHandler()
    handlers = [streamHandler] if DRY_RUN else [fileHandler,streamHandler]

    
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(levelname)s: %(message)s',
                       handlers=handlers)

    
    
    main()

