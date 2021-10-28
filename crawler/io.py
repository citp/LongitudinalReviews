import pandas as pd
import safer
import os
import shutil
import zipfile
import json
import logging
import glob

logger = logging.getLogger("io")

def get_yelpzip_names():
    prod_mapping = pd.read_csv("YelpZip-Data/productIdMapping.txt", delimiter="\t",
                               names=["name","prod_id"])
    return set(prod_mapping.name)

def get_crawled_reviews_for_mukherjee_recrawl():
    #Check if we already have some reviews
    path = "data/mukherjee_recrawl_reviews.json"
    not_recommended_path = "data/mukherjee_recrawl_not_recommended_reviews.json"
    if os.path.exists(path):
        try:
            with open(path) as f:
                reviews = json.load(f)
        except:
            logger.error("Failed to load reviews. Backing up.", exc_info=True)
            shutil.copy(path, "%s.bak" % path)
            reviews = {}
        logger.debug("Had reviews for Mukherjee recrawl, using old reviews")
    else:
        reviews = {}
        logger.debug("No reviews for Mukherjee recrawl")
    if os.path.exists(not_recommended_path):
        try:
            with open(not_recommended_path) as f:
                not_recommended_reviews = json.load(f)
        except:
            logger.error("Failed to load not recommended reviews. Backing up.", exc_info=True)
            shutil.copy(not_recommended_path, "%s.bak" % not_recommended_path)
            not_recommended_reviews = {}
        logger.debug("Had not recommended reviews for Mukherjee recrawl, using old reviews")
    else:
        not_recommended_reviews = {}
        logger.debug("No not recommended reviews for Mukherjee recrawl")
    return reviews, not_recommended_reviews

def save_reviews_mukherjee_recrawl(reviews, not_recommended_reviews):

    if not SAVE:
        logger.info("Saving disabled.")
        return
    
    #Save reviews for zipcode
    path = "data/mukherjee_recrawl_reviews.json"
    not_recommended_path = "data/mukherjee_recrawl_not_recommended_reviews.json"

    #Backup in case save fails
    logger.debug("Backing up old dataset files")
    try:
        backup(path)
        backup(not_recommended_path)
    except:
        logger.warning("Failed to backup old dataset", exc_info=True)
    
    #Save
    logger.debug("Writing dataset")
    with safer.open(path, "w+",temp_file=True) as f:
        json.dump(reviews,f)
    with safer.open(not_recommended_path, "w+",temp_file=True) as f:
        json.dump(not_recommended_reviews,f)

    #Log
    logger.debug("Wrote %d,%d businesses for Mukherjee recrawl" % (len(reviews),len(not_recommended_reviews)))

def find_all_crawled_zipcodes(data_folder=None):
    if data_folder is None:
        raise Exception("Data folder must be defined")
    
    found_zips = set()
    
    recommended_path = f"{data_folder}/recommended_reviews/"
    not_recommended_path = f"{data_folder}/not_recommended_reviews/"
    removed_path = f"{data_folder}/removed_reviews/"
    for path in [recommended_path, not_recommended_path, removed_path]:
        if not os.path.exists(path):
            #logger.warning(f"Path does not exist: {path}")
            continue
        for file in os.listdir(path):
            if not os.path.isfile(os.path.join(path,file)):
                continue
            if file.endswith(".json"):
                found_zips.add(file[:-5])
                
    return list(found_zips)

def get_crawled_reviews_for_zipcode(zipcode=None,data_folder=None,load_business_data=True):
    if data_folder is None:
        raise Exception("Data folder must be defined")
    
    #Check if we already have some reviews
    business_data_path = f"{data_folder}/business_data/{zipcode}.json"
    recommended_path = f"{data_folder}/recommended_reviews/{zipcode}.json"
    not_recommended_path = f"{data_folder}/not_recommended_reviews/{zipcode}.json"
    removed_path = f"{data_folder}/removed_reviews/{zipcode}.json"

    paths = []
    if load_business_data:
        paths.append(business_data_path)
    paths += [recommended_path, not_recommended_path, removed_path]

    review_dicts = []
    for path in paths:
        if os.path.exists(path):
            try:
                with open(path) as f:
                    reviews = json.load(f)
            except:
                logger.error("Failed to load reviews. Backing up.", exc_info=True)
                shutil.copy(recommended_path, "%s.bak" % path)
                reviews = {}
            logger.debug("Had reviews for zipcode, using old reviews")
        else:
            reviews = {}
            logger.debug("No reviews for zipcode")
        review_dicts.append(reviews)
        
    return tuple(review_dicts)

def backup(path):
    i = 0
    if not os.path.exists(path):
        return
    while True:
        fn = "%s.%d.bak" % (path,i)
        if not os.path.exists(fn):
            backup_path = shutil.copyfile(path,fn)
            return
        i += 1

def save_reviews_zipcode(zipcode=None, business_data=None, recommended_reviews=None, not_recommended_reviews=None,removed_reviews=None,data_folder=None):
    
    if data_folder is None:
        raise Exception("Data folder must be defined")
    
    #Save reviews for zipcode
    business_data_path = f"{data_folder}/business_data/{zipcode}.json"
    recommended_path = f"{data_folder}/recommended_reviews/{zipcode}.json"
    not_recommended_path = f"{data_folder}/not_recommended_reviews/{zipcode}.json"
    removed_path = f"{data_folder}/removed_reviews/{zipcode}.json"
    
    
    for reviews, path in zip([business_data, recommended_reviews,not_recommended_reviews,removed_reviews], [business_data_path, recommended_path, not_recommended_path, removed_path]):
        os.makedirs(os.path.dirname(path),exist_ok=True)
            
        #Save
        with safer.open(path, "w+",temp_file=True) as f:
            json.dump(reviews,f)
        
    #Log
    logger.debug(f"Wrote {len(reviews)},{len(not_recommended_reviews)},{len(removed_path)} businesses for zipcode {zipcode}")

def get_zipcode_businesses(zipcode=None):

    fn = f"data/businesses_by_zipcode/{zipcode}.json"
    
    if not os.path.exists(fn):
        #If the file doesn't exist, assume there were not businesses in that zipcode
        logger.warning(f"No businesses for zipcode {zipcode}")
        return
    
    with open(fn) as f:
        businessid_map = json.load(f)
    for business in businessid_map:
        try:
            yield business["url"], business["id"]
        except:
            logger.debug(business)
            raise
        
        

                
def get_mukherjee_businesses():
    with open("data/businessid_to_data.json") as f:
        businessid_map = json.load(f)
    for bid in businessid_map:
        try:
            if "error" in businessid_map[bid]:
                continue
            yield businessid_map[bid]["url"], bid
        except:
            logger.debug(businessid_map[bid])
            raise
        
        
        
MAX_BACKUP_SIZE = 2 * 10**9 #2 GB
def compress_raw_data(zipcode=None,business_id=None,data_folder=None,crawl_id="",max_backup_size=MAX_BACKUP_SIZE):
    if data_folder is None:
        raise Exception("Data folder must be defined")
    
    logger.info("Compressing files")
    
    html_docs_folder = f"{data_folder}/raw_html/{business_id}"
    compressed_data_file = f"{data_folder}/raw_html_{zipcode}.zip"
    backup_compressed_data_file = f"{compressed_data_file}.bak"

    mode = "a"

    if False and os.path.getsize(compressed_data_file) > max_backup_size:
        i = 0
        while not os.path.exists(partfn :=  f"{data_folder}/raw_html_{zipcode}_part{i}.zip"):
            i += 1

        logger.info("Compressed file too large, making a new file")
            
        os.rename(compressed_data_file,partfn)
        

    if os.path.exists(backup_compressed_data_file):
        logger.error("Backup file exists, quitting to avoid overwriting backup")
        killswitch=True
        raise SystemExit()

#    if os.path.exists(compressed_data_file):
#        logger.info("Making backup")
#        shutil.copy(compressed_data_file, backup_compressed_data_file)

    logger.info("Adding files to ZIP")
    with zipfile.ZipFile(compressed_data_file, mode=mode, compression=zipfile.ZIP_DEFLATED) as zf:
        zipped_files = set(zf.namelist())
        for os_path in sorted(glob.glob(f"{html_docs_folder}/**",recursive=True)):

            if not os.path.isfile(os_path): continue
            
            archive_path = os.path.relpath(os_path,start=data_folder)
            
            logger.info(f"Adding {os_path} to archive as {archive_path}")
            
            if archive_path in zipped_files:
                with open(os_path) as f:
                    text = f.read()
                    if text != zf.open(archive_path).read():
                        logger.error("Mismatched archive file data!")
                        with open(f"logs/{crawl_id}/zip_fail_external.html","w+") as fn:
                            fn.write(text)
                        with open(f"logs/{crawl_id}/zip_fail_internal.html","w+") as fn:
                            fn.write(zf.open(archive_path).read().decode('utf-8'))
                        #killswitch=True
                        #raise SystemExit()
                
                logger.info(f"Archive already contains file, skipping")
            else:
                zf.write(os_path, archive_path)

    logger.info("Removing uncompressed folders and backup file")
    shutil.rmtree(html_docs_folder)

#    if os.path.exists(backup_compressed_data_file):
#        logger.info("Removing backup")
#        os.remove(backup_compressed_data_file)

    logger.info("Successfully removed all uncompressed files and backup file")

