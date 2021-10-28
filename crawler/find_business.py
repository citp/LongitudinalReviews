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
import safer

import crawler.categories

#Singletons
logger = logging.getLogger("find_business")
zipcodedb = pyzipcode.ZipCodeDatabase()

#Constants
LIMIT = 50
FETCH_LIMIT = 1000
CALL_LIMIT = 3500

NON_EXISTENT_ERROR = "{'error': {'code': 'LOCATION_NOT_FOUND', 'description': 'Could not execute search, try specifying a more exact location.'}}"

def find_businesses_with_constraints(zipcode,constraints={}):
    """
constraints is a dictionary of string -> list of strings
    """

    logger.debug("Zipcode: %s" % zipcode)
    
    #Flatten the dictionary into a list of lists. Each sub-list represents one named constraint
    #Each sub-list contains a tuple of (name,value)
    constraints_ar = [
        [(constraint,value) for value in constraints[constraint]]
        for constraint in constraints["order"]
    ]
    #Try just a subset of the constraints
    first_error = None
    for n_constraints in range(len(constraints_ar)+1):
        logger.debug("Trying %d constraints" % n_constraints)
        
        attempt_constraints = list(itertools.product(*constraints_ar[:n_constraints]))

        failed = False
        business_accumulator = []
        business_ids = set()

        #First round -- check just the first page
        params_ar = []
        for round_constraints in attempt_constraints:
            api_args = {
                name:value for name,value in round_constraints
            }

            logger.debug("Trying args: %s" % ",".join("%s=%s" % t for t in api_args.items()))

            #Fetch with the constraints
            params,error = check_businesses(zipcode,api_args)
            if first_error is None:
                first_error = error

            #Error -- handle
            if params is None:
                #Severe error -- fail
                if type(error) is not int:
                    return None,error
                #Not narrow enough -- need to try more constraints
                failed=True
                break

            params_ar.append(params)

        if failed:
            continue

        logger.debug("Confirmed -- captures entire set")
        
        #Second round -- process first page & do subsequent pages
        for params,round_constraints in zip(params_ar,attempt_constraints):
            api_args = {
                name:value for name,value in round_constraints
            }

            logger.debug("Fetching args: %s" % ",".join("%s=%s" % t for t in api_args.items()))

            #Fetch with the constraints
            businesses,error = find_businesses(zipcode,api_args,params=params)
            if first_error is None:
                first_error = error

            #Error -- handle
            if businesses is None:
                #Severe error -- fail
                if type(error) is not int:
                    return businesses,error
                #Not narrow enough -- need to try more constraints
                failed=True
                break
            start_ct = len(business_accumulator)
            for business in businesses:
                business_id = business["id"]
                if business_id in business_ids:
                    continue
                business_ids.add(business_id)
                business_accumulator.append(business)
            logger.debug("Found %d new businesses" % (len(business_accumulator) - start_ct))
        if not failed:
            logger.debug("Success. Found %d total businesses" % len(business_accumulator)) 
            return business_accumulator,first_error
    return None,first_error
                    

def check_businesses(zipcode,api_args):
    """
Fetches the first page to check if it will fail, so you don't have to download the whole batch to know if one will fail
    """
    api_args["location"]=str(zipcode)
    api_args["limit"]=str(50)
    if "categories" not in api_args:
        api_args["categories"]="restaurants"

    #Make initial fetch
    response = yelp_api.search(**api_args)

    for i in range(5):
        total = response.get("total")
        if total != None: #We have the total
            break

        #We don't -- error handle

        #Get error code
        code = ""
        try:
            code = response.get("error").get("code")
        except:
            pass

        #Run out of API calls
        if code == "ACCESS_LIMIT_REACHED":
            #Quit
            raise Exception("Access limit reached")

        #Temporary error
        if code == "INTERNAL_ERROR":
            #Backoff if it's not the last loop
            if i < 4:
                logger.debug("Internal Error -- Retrying")
                time.sleep(5)
                response = yelp_api.search(offset="0",**api_args)
                continue

        #Not sure why we failed, skip this businesses
        logger.error("No businesses. Dump: %s" % str(new_businesses))
        return None,str(response)

    #Yelp limits us to 1,000 requests. Handle cases where there are more than 1k businesses
    if total > FETCH_LIMIT:
        logger.warning("Zipcode %s has %d businesses. Can only fetch %d." % (zipcode, total, FETCH_LIMIT))
        return None,total
    max_offset = min(total,FETCH_LIMIT)
    return response,total
    

#We can add more constraints later if we need to filter many zipcodes
def find_businesses(zipcode,api_args=None,params=None,ignore_limit=False):
    """
api_args should be a dictionary of strings passed to the yelp API, beyond what's already included
    """

    if api_args is None:
        api_args = {}
    

    num_args = len(api_args)
    
    api_args["location"]=str(zipcode)
    api_args["limit"]=str(50)
    if "categories" not in api_args:
        api_args["categories"]="restaurants"

        
    if params is None:
        #Make initial fetch
        response = yelp_api.search(**api_args)
    else:
        #We already have it cached
        response = params
        
    #Check how many entries there are
    total = response.get("total")

    logger.info(f"Total {total}")

    if total is None:
        code = ""
        try:
            code = response.get("error").get("code")
        except:
            pass
        if code == "ACCESS_LIMIT_REACHED":
            raise Exception("Access limit reached")
        logger.error("No total. Response: %s" % str(response))
        logger.error("Zipcode: %s" % str(zipcode))
        return None,str(response)

    #Yelp limits us to 1,000 requests. Handle cases where there are more than 1k businesses
    if total > FETCH_LIMIT:
        logger.warning("Zipcode %s has %d businesses. Can only fetch %d." % (zipcode, total, FETCH_LIMIT))
        if not ignore_limit:
            return None,total
    max_offset = min(total,FETCH_LIMIT)

    #Iterate through all of the pages, starting with the page we just downloaded
    offset = 0
    index = 0
    businesses = []
    while True:

        #Get businesses
        for i in range(5):
            new_businesses = response.get("businesses")
            if new_businesses != None: #We have them
                break

            #We don't -- error handle

            #Get error code
            code = ""
            try:
                code = response.get("error").get("code")
            except:
                pass

            #Run out of API calls
            if code == "ACCESS_LIMIT_REACHED":
                #Quit
                raise Exception("Access limit reached")

            #Temporary error
            if code == "INTERNAL_ERROR":
                #Backoff if it's not the last loop
                if i < 4:
                    logger.debug("Internal Error -- Retrying")
                    time.sleep(5)
                    response = yelp_api.search(offset=str(offset),**api_args)
                    continue

            #Not sure why we failed, skip this businesses
            logger.error("No businesses. Dump: %s" % str(new_businesses))
            return None,str(response)

        
        #Record the ranking
        #But only if we weren't given any args to start with
        if num_args == 0:
            
            logging.debug(f"Doing indexing for zipcode {zipcode}")
            for business in new_businesses:
                business["index_%s" % zipcode] = (index,total)
                index += 1
        else:
            logging.debug(f"Not indexing for zipcode {zipcode}; Number of args: {num_args}; Arguments: {api_args}")
                
        
        businesses += new_businesses

        offset += LIMIT #Next page

        #Check if we've reached the last page
        if offset >= max_offset:
            break
        
        #Fetch next page
        response = yelp_api.search(offset=str(offset),**api_args)
        
    return businesses,total

def get_zipcodes(states=["NY","CT","NJ","PA","VT"], use_chicago_zipcodes=False, preset_zips=None):

    if preset_zips:
        zips = preset_zips
    elif use_chicago_zipcodes:
        zips = ['60611', '60614', '60302', '60610', '60607', '60657', '60605',
                '60616', '60654', '60603', '60602', '60631', '60601', '60606',
                '60661', '60660', '60615', '60647', '60618', '60622', '60640',
                '60625', '60642', '60613', '60641', '60646', '60608', '60630',
                '60629', '60623', '60659', '60612', '60609', '60632', '60638',
                '60644', '60643', '60546']
    else:    
        zips = sum((zipcodedb.find_zip(state=state) for state in states), start=[])
        zips = [zipcode.zip for zipcode in zips]
    return zips

def get_local_zipmap(zipcode):
    path = "data/businesses/%s.json" % zipcode
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)

def save_local_zipmap(zipcode, zip_map):
    path = "data/businesses/%s.json" % zipcode
    with safer.open(path, "w+",temp_file=True) as f:
        json.dump(zip_map,f)

    
def fetch_businesses(zips, failure_map = {}, constraints={}):

    #Go through each zipcode
    for zipcode in zips:

        #Check if we've hit the daily call limit
        if yelp_api.calls_made >= CALL_LIMIT:
            break

        zip_map = get_local_zipmap(zipcode)
        changed = False

        try:
            #Check if we already have results
            if (zipcode in zip_map) or (zipcode in failure_map and (failure_map[zipcode] == NON_EXISTENT_ERROR)):
                if zipcode in zip_map:
                    t = len(zip_map[zipcode])
                else:
                    t = failure_map[zipcode]
                logger.debug("Already have %s, skipping. Reason: %s" % (zipcode, t))
                continue
            reason = "Not yet fetched"
            if zipcode in failure_map:
                value = failure_map[zipcode]
                reason = "Past error: %s" % value
            elif zipcode in zip_map:
                reason = "Had %d items" % len(zip_map[zipcode])
            logger.debug("Fetching businesses for zipcode %s. Reason: %s" % (zipcode,reason))
            if DRY_RUN:
                continue

            #Download business info
            businesses,error_msg = find_businesses_with_constraints(zipcode,constraints=constraints)
            if businesses is not None:
                #Success
                logger.debug(f"Saving {len(businesses)} businesses for zipcode {zipcode}")
                zip_map[zipcode] = businesses
                changed = True
                if zipcode in failure_map:
                    del failure_map[zipcode]
            else:
                #Fail
                logger.debug("Noting failure for zipcode %s; msg: %s" % (zipcode, str(error_msg)))
                failure_map[zipcode] = error_msg
        finally:
            if not DRY_RUN and changed:
                save_local_zipmap(zipcode, zip_map)
                

def main():

    constraints = {
        "order": ["price", "categories"],
        "categories": list(crawler.categories.get_grouped_categories(10)),
        "price": ["1","2","3","4"]
    }
    
    #constraints={
    #    "order": ["price", "open_now"],
    #    "price": ["1","2","3","4"],
    #    "open_now": ["true","false"]
    #}

    zips = get_zipcodes(use_chicago_zipcodes=CHICAGO_ZIPCODES,preset_zips = zipcodes)
#    try:
#        zip_map = json.load(open("data/zip_map.json"))
#        logging.debug("Loaded businesses")
#    except:
#        zip_map = {}
#        logging.debug("Failed to load old businesses")
    try:
        failure_map = json.load(open("data/failure_map.json"))
        logging.debug("Loaded failures")

        zips = sorted(zips, key=lambda z: failure_map[z] if z in failure_map and type(failure_map[z]) is int else 1000000)
        
    except:
        failure_map = {}
        logging.debug("Failed to load old failures")

    #zips = [zipcode for zipcode in failure_map if type(failure_map[zipcode]) is int]

    #zips = zips[:1]
    #logger.debug("Zipcodes: %s" % str(zips))
    
    try:
        logging.info("Fetching")
        fetch_businesses(zips, failure_map=failure_map,constraints=constraints)
    finally:
        logging.info("Saving")
        if not DRY_RUN:
            with safer.open("data/failure_map.json","w+",temp_file=True) as f:
                json.dump(failure_map, f)

def ranking_mode():
    try:
        zip_map = json.load(open("data/ranking_zip_map.json"))
        logging.debug("Loaded businesses")
    except:
        zip_map = {}
        logging.debug("Failed to load old businesses")
        
    try:
        failure_map = json.load(open("data/ranking_failure_map.json"))
        logging.debug("Loaded failures")

        zips = sorted(zips, key=lambda z: failure_map[z] if z in failure_map and type(failure_map[z]) is int else 1000000)
        
    except:
        failure_map = {}
        logging.debug("Failed to load old failures")

    #zips = [zipcode for zipcode in failure_map if type(failure_map[zipcode]) is int]

    #zips = zips[:1]
    #logger.debug("Zipcodes: %s" % str(zips))
    
    try:
        logging.info("Fetching")
        for zipcode in zips:
            logging.info(f"Fetching for zipcode {zipcode}")
            if zipcode in zip_map and not OVERWRITE:# or zipcode in failure_map:
                continue
            
            businesses, error_msg = find_businesses(zipcode, ignore_limit=True)

            if businesses is not None:
                zip_map[zipcode] = businesses
            else:
                logging.error(error_msg)
                failure_map[zipcode] = error_msg
    finally:
        if DRY_RUN:
            logging.info("Dry run, not saving")
        else:
            logging.info("Saving")
            with safer.open("data/ranking_failure_map.json","w+",temp_file=True) as f:
                json.dump(failure_map, f)
            with safer.open("data/ranking_zip_map.json","w+",temp_file=True) as f:
                json.dump(zip_map, f)
    
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get YELP businesses')
    parser.add_argument('--dry_run', dest='dry_run', action='store_const',
                    const=True, default=False,
                    help='Do a dry run')
    parser.add_argument('--call_limit', type=int, default=4500,
                    help='Max number of API calls')
    parser.add_argument('--ranking_mode', action='store_const',
                    const=True, default=False,
                    help='Collect rankings by zipcode')
    parser.add_argument('--overwrite', action='store_const',
                    const=True, default=False,
                    help='Overwrite existing data')
    parser.add_argument('--chicago_zip', dest='chicago_zip', action='store_const',
                    const=True, default=False,
                    help='Fetch from Chicago Zipcodes.')
    parser.add_argument('zipcodes', nargs='*', help='(Optional) zipcodes to collect')


    

    args = parser.parse_args()
    DRY_RUN = args.dry_run
    CALL_LIMIT = args.call_limit
    OVERWRITE = args.overwrite
    CHICAGO_ZIPCODES = args.chicago_zip
    zipcodes = args.zipcodes
    if len(zipcodes) == 0:
        zipcodes = None
    
    fileHandler = logging.FileHandler("logs/yelp_api_fetch_%s.txt" % time.strftime("%Y%m%d-%H%M"))
    streamHandler = logging.StreamHandler()
    handlers = [streamHandler] if DRY_RUN else [fileHandler,streamHandler]

    
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(levelname)s: %(message)s',
                       handlers=handlers)

    
    if args.ranking_mode:
        ranking_mode()
    else:
        main()
