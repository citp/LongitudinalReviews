import os
import enum
import itertools

CHICAGO_ZIPCODES = "60611 60610 60605 60603 60601 60660 60618 60625 60641 60630 60659 60632 60643 60614 60607 60616 60602 60606 60615 60622 60642 60646 60629 60612 60638 60546 60302 60657 60654 60631 60661 60647 60640 60613 60608 60623 60609 60644".split(" ")
EXTENDED_ZIPCODES = "23222 60523 17320 58202 98605 33710 34240 37025 59020 90290 44857 63026 28726 60004 61084 27587 99143 93226 87048 98235 11372 18930 28571 61032 11762 79511 57006 19096 14847 95966 80620 85746 93634 86038 68633 35749 36611 40440 17048 67480 85296 97911 17748 37115 08824 79848 06235 25827 07853 02669 12461 93446 52404 93626 65722 97498 22314 60620 37762 37010 59105 19066 98030 57232 57567 72751 76225 59442 42048 61319 22973 17047 31027 97635 21250 19054 56171 89825 32608 44667 15622 20110 20710 27944 90018 70533 65560 13327 39421 80021 74849 57035 50322 60171 92562 66208 33023 78957 02645 71411 66062 10154 35131 88210 59275 27502 30817 98110 81144 50276 50638 21930 79772 27242 14843 58005 64473 75061 30552 15832 44833 08848 03263 02562 14131 02809 15676 80736 67127 15412 56477 90623 31003 10475 57064 28365 34208 30442 19081 93402 04001 12086 05761 07011 73055 90240 38541 01089 89118 80107 15470 39574 14739 48835 60409 77005 21734 44870 19103 54013 45504 62056 08752 54411 17370 39440 17976 23947 50105 21045 11419 32829 14040 14143 37019 08830 51040 01460 17339 81047 75563 49920 73063 87556 93424 55025 51246 90021 67353 13618 72116 24925 16301 49733 37814 33020 15135 76301 71929 41086 35208 32779 81052 48122 32351 37213 06066 27921 64156 98822 74105 85298 82222".split(" ")


#CRAWL_ORDER_CHICAGO = ["crawl_9", "crawl_10", "crawl_11", "crawl_12", "crawl_13", "crawl_14", "crawl_15"]
CRAWL_ORDER_CHICAGO = ["crawl_10", "crawl_11", "crawl_12", "crawl_13", "crawl_14", "crawl_15", "crawl_16", "crawl_17", "crawl_18"]
CRAWL_ORDER_EXTENDED = ["crawl_x0", "crawl_x1", "crawl_x2", "crawl_x3"]

DATA_DIR = "../../data/"
BUSINESSES_DIR = os.path.join(DATA_DIR, "businesses_by_zipcode")

#Crawls
CRAWL_DIR = os.path.join(DATA_DIR, "%s")
RECOMMENDED_DIR = os.path.join(CRAWL_DIR, "recommended_reviews")
NOT_RECOMMENDED_DIR = os.path.join(CRAWL_DIR, "not_recommended_reviews")
REMOVED_DIR = os.path.join(CRAWL_DIR, "removed_reviews")
BUSINESS_DATA_DIR = os.path.join(CRAWL_DIR, "business_data")

#Census
CENSUS_DIR = os.path.join(DATA_DIR, "census_data")
CENSUS_2019_GAZETTEER = os.path.join(CENSUS_DIR, "2019_Gaz_zcta_national.txt")
CENSUS_2019_ZCTA_POPULATION = os.path.join(CENSUS_DIR, "ACSDT5Y2019.B01003_data_with_overlays_2021-05-11T142846.csv")
CENSUS_2019_ZCTA_INCOME = os.path.join(CENSUS_DIR, "ACSST5Y2019.S1901_data_with_overlays_2021-05-14T104841.csv")

#Pickles
PICKLES_DIR = os.path.join(DATA_DIR, "pickles")
LONG_DATA_FILE_TMPL = os.path.join(PICKLES_DIR, "%s", "longitudinal_reviews.pkl")
RECALSSIFICATION_DATA_FILE_TMPL = os.path.join(PICKLES_DIR, "%s", "reclassification_data.pkl")
AUTHOR_MATCH_FILE_TMPL = os.path.join(PICKLES_DIR, "%s", "author_match.pkl")
CRAWLED_BUSINESS_DATA_FILE_TMPL = os.path.join(PICKLES_DIR, "%s", "crawled_business_data.pkl")
BUSINESS_DATA_FILE = os.path.join(PICKLES_DIR, "business_data.pkl")

#Crawl source?
CRAWL_SOURCE_CHICAGO = "chicago"
CRAWL_SOURCE_EXTENDED = "usa"

#JSON
JSON_DIR = os.path.join(DATA_DIR, "json")
MISSING_MASK_DATA_TMPL = os.path.join(JSON_DIR, "%s", "missing_mask_data.json")
REPLACEMENT_MASK_DATA_TMPL = os.path.join(JSON_DIR, "%s", "replacement_mask_data.json")


ZIPCODE_TO_STRATA = os.path.join(PICKLES_DIR, "zipcode_to_income_density_strata.pkl")
CENSUS_STRATA_DATA = os.path.join(PICKLES_DIR, "census_with_strata.pkl")

class CrawlExperiment:
    CHICAGO = 1
    DENSITY = 2
    INCOME = 3
    
    def __init__(self,value):
        self.value = value
        
#Experiment -> Stratum -> Name
STRATA_COMMON_NAMES = {
    CrawlExperiment.CHICAGO: {0: ""},
    CrawlExperiment.DENSITY: {0: "1.9e3 - 5.8e4 ppl/km^2", 1: "8.8e2 - 1.9e3 ppl/km^2", 2: "3.0e2 - 8.8e2 ppl/km^2", 3: "67 - 3.0e2 ppl/km^2", 4: "0-67 ppl/km^2"},
    CrawlExperiment.INCOME:  {0: "$105k - $250k", 1: "$82k-$105k", 2: "$68k-$82k", 3: "$55k-$68k", 4: "$0-55k"}
}

STRATA_COMMON_NAMES_HUE_ORDER = [STRATA_COMMON_NAMES[CrawlExperiment.CHICAGO][i] for i in range(1)] + [STRATA_COMMON_NAMES[CrawlExperiment.DENSITY][i] for i in range(5)] + [STRATA_COMMON_NAMES[CrawlExperiment.INCOME][i] for i in range(5)]

COMBINED_STRATUM_COMMON_NAMES = {i: f"{STRATA_COMMON_NAMES[CrawlExperiment.DENSITY][i]}/{STRATA_COMMON_NAMES[CrawlExperiment.INCOME][i]}" for i in range(5)}
COMBINED_STRATUM_COMMON_NAMES_HUE_ORDER = [COMBINED_STRATUM_COMMON_NAMES[i].replace("$","\\$") for i in range(5)]


def set_crawl_source(crawl_source=CRAWL_SOURCE_CHICAGO):
    global CRAWL_SOURCE, CRAWL_ORDER, CRAWL_NUMBER, LONG_DATA_FILE, RECALSSIFICATION_DATA_FILE, AUTHOR_MATCH_FILE, ZIPCODES, MISSING_MASK_DATA, REPLACEMENT_MASK_DATA, CRAWLED_BUSINESS_DATA_FILE 
    
    CRAWL_SOURCE = crawl_source
    
    if crawl_source == CRAWL_SOURCE_CHICAGO:
        CRAWL_ORDER = CRAWL_ORDER_CHICAGO
        ZIPCODES = CHICAGO_ZIPCODES
    elif crawl_source == CRAWL_SOURCE_EXTENDED:
        CRAWL_ORDER = CRAWL_ORDER_EXTENDED
        ZIPCODES = EXTENDED_ZIPCODES
    else:
        raise Exception(f"Illegal argument: {crawl_source}")
    
    CRAWL_NUMBER = {
        crawl_id: i for i, crawl_id in enumerate(CRAWL_ORDER)
    }
    
    #Create parent directories
    try:
        os.makedirs(os.path.join(JSON_DIR, crawl_source))
        os.makedirs(os.path.join(PICKLES_DIR, crawl_source))
    except:
        pass
    
    #Fill in templated directories
    LONG_DATA_FILE = LONG_DATA_FILE_TMPL % crawl_source
    RECALSSIFICATION_DATA_FILE = RECALSSIFICATION_DATA_FILE_TMPL % crawl_source
    AUTHOR_MATCH_FILE = AUTHOR_MATCH_FILE_TMPL % crawl_source
    MISSING_MASK_DATA = MISSING_MASK_DATA_TMPL % crawl_source
    REPLACEMENT_MASK_DATA = REPLACEMENT_MASK_DATA_TMPL % crawl_source
    CRAWLED_BUSINESS_DATA_FILE = CRAWLED_BUSINESS_DATA_FILE_TMPL % crawl_source
    
set_crawl_source(CRAWL_SOURCE_CHICAGO)