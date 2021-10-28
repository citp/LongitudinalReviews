"""
Yelp's Fusion API gives us business recommendations from a zipcode query.
Many of these businesses lie outside the desired zipcode. Therefore, we need to
load all of the businesses and organize them by location zipcode.
"""
import json
import json
import os
import pandas as pd
import collections
import seaborn as sns
import math
import pyzipcode
import hashlib
from tqdm import tqdm
import crawler.find_business as find_business


business_file = "data/businesses/%s.json"

def load_businesses(zipcode, loaded_ids):
    if not os.path.exists(business_file % zipcode):
        return
    with open(business_file % zipcode) as f:
        zipcode_data = json.load(f)
    for business in zipcode_data[zipcode]:
        bid = business["id"]
        if bid not in loaded_ids:
            loaded_ids.add(bid)
            yield business

def invert(zipcodes):
    bids = set()
    businesses = []
    for zipcode in tqdm(zipcodes,total=len(zipcodes)):
        for business in load_businesses(zipcode, bids):
            businesses.append(business)


    try:
        os.mkdir("data/businesses_by_zipcode/")
    except:
        print("exists")
        pass
    try:
        os.mkdir("data/not_recommended_reviews/")
    except:
        print("exists")
        pass
    try:
        os.mkdir("data/recommended_reviews/")
    except:
        print("exists")
        pass

    businesses_by_zipcode = collections.defaultdict(list)
    for business in businesses:
        businesses_by_zipcode[business["location"]["zip_code"]].append(business)
    
    for zipcode in businesses_by_zipcode:
        with open("data/businesses_by_zipcode/%s.json" % zipcode, "w+") as f:
            json.dump(businesses_by_zipcode[zipcode], f)

if __name__ == "__main__":
    zipcodes = find_business.get_zipcodes(use_chicago_zipcodes=True)
    invert(zipcodes)
