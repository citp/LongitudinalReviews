import logging
import collections
import argparse

from crawler.io import get_crawled_reviews_for_zipcode, find_all_crawled_zipcodes

CHICAGO_CRAWLS = ["crawl_9", "crawl_10", "crawl_11", "crawl_12", "crawl_13", "crawl_14", "crawl_15"]

def merge_reviews(business_lists):
    ret = collections.defaultdict(list)
    for bl in business_lists:
        for business in bl:
            ret[business].extend(bl[business])
    return ret

def load_all_reviews(crawl_id):
    data_folder = f"data/{crawl_id}"
    zipcodes = find_all_crawled_zipcodes(data_folder=data_folder)
    return {zipcode: merge_reviews(get_crawled_reviews_for_zipcode(zipcode=zipcode, data_folder=data_folder, load_business_data=False)) for zipcode in zipcodes}

def compare_reviews(new_reviews, reference_reviews):
    
    review_count = 0
    missing_reviews = 0
    
    def describe_review_1(review):
        try:
            return (review["date"], review["rating"], review["user_name"])
        except:
            print(review)
            raise

    def describe_review_2(review):
        return (review["date"], review["rating"], review["content"])
    
    for zipcode in reference_reviews:
        for business in reference_reviews[zipcode]:
            try:
                review1 = None
                review2 = None
                new_review_set_1 = set((describe_review_1(review1) for review1 in new_reviews[zipcode][business]))
                new_review_set_2 = set((describe_review_2(review1) for review1 in new_reviews[zipcode][business]))
                for review2 in reference_reviews[zipcode][business]:
                    review_count += 1
                    if (describe_review_1(review2) not in new_review_set_1) and (describe_review_2(review2) not in new_review_set_2):
                        missing_reviews += 1
            except:
                print(f"{zipcode},{business},{review1},{review2}")
                raise
    return missing_reviews, review_count

def find_reappearances():
    pass

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Run sanity checks')
    
    parser.add_argument('--check', dest='check', default="missing", type=str, help="Which check to run. 'missing' 'reappearances'")
    parser.add_argument('--crawl_id', dest='crawl_id', default=None, type=str, help="Crawl Identifier for crawl in question")
    parser.add_argument('--ref_crawl_id', dest='ref_crawl_id', default=None, type=str, help="Crawl Identifier reference crawl")
    
    
    args = parser.parse_args()
    
    if args.check == "missing":
        ct_missing, ct_all = compare_reviews(load_all_reviews(args.crawl_id), load_all_reviews(args.ref_crawl_id))
        print(f"{ct_missing/ct_all}",end=' ')
    elif args.check == "reappearances":
        pass
