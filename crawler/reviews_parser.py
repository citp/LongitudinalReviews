from cssselect import GenericTranslator
import re
import ratelimit
from lxml import etree
from lxml.etree import XPath
import lxml,lxml.html
import logging
import json

logger = logging.getLogger("reviews_parser")
logger.setLevel(logging.INFO)

#Sleep rules
N_FETCH = 15
FETCH_SLEEP = 1

#Navigation selectors
NEXT_PAGE = "a.next-link" #"div:last-child > span > a > span.navigation-button-icon__373c0__2Fl7a > svg"
sel_next_page = XPath(GenericTranslator().css_to_xpath(NEXT_PAGE))
BANNED_TEXT = "Sorry, you’re not allowed to access this page."
NOT_RECOMMENDED_NEXT_PAGE = "div.not-recommended-reviews div.arrange_unit > a.next"
REMOVED_NEXT_PAGE = "div.removed-reviews div.arrange_unit > a.next"
sel_unusual_activity = XPath("//*[text() = 'Unusual Activity Alert']|//*[text() = 'Public Attention Alert']")
close_unusual_activity = "//*[text() = 'Got it, thanks!']"

#Business data
#CSS selectors
sel_raw_data_container = XPath(GenericTranslator().css_to_xpath('script[data-apollo-state]')) 
rd_left_marker = b"&lt;!--"
rd_right_marker = b"--&gt;"

#Strings
ammenity_prefix = ".organizedProperties({\"clientPlatform\":\"WWW\"}).0.properties" #Not strictly a prefix
ammenity_keys = ["displayText","alias","isActive","iconName"]


#Recommended reviews
#CSS selectors
sel_review_container = XPath(GenericTranslator().css_to_xpath("section"))
sel_page_number = XPath(GenericTranslator().css_to_xpath('div[aria-label="Pagination navigation"] > div > span'))
sel_review_list = XPath(GenericTranslator().css_to_xpath('div.border-color--default__373c0__1WKlL > ul.undefined.list__373c0__vNxqp'))
#sel_review_list = XPath(GenericTranslator().css_to_xpath('ul.lemon--ul__373c0__1_cxs.undefined.list__373c0__2G8oH'))
sel_user_card = XPath(GenericTranslator().css_to_xpath('div.user-passport-info'))
sel_rating = XPath('descendant-or-self::div/@aria-label')
sel_elite = XPath('descendant-or-self::a[@href="/elite"]')
sel_review = XPath(GenericTranslator().css_to_xpath('p > span.raw__373c0__tQAx6'))
#sel_review_count = XPath('descendant-or-self::span[contains(@class, "icon--16-review-v2")]/..//span[2]')
sel_review_count = XPath(GenericTranslator().css_to_xpath('span.css-bq71j2'))
#sel_review_count = XPath(GenericTranslator().css_to_xpath('div:has(> span.icon--16-review-v2) > span.text__373c0__2Kxyz '))
#sel_review_count = XPath(GenericTranslator().css_to_xpath('div.photoHeader__373c0__YdvQE span.text__373c0__2Kxyz'))
sel_date = XPath(GenericTranslator().css_to_xpath('span.css-e81eai'))
sel_user_friend_count = XPath(GenericTranslator().css_to_xpath('div[aria-label="Friends"]'))
sel_user_review_count = XPath(GenericTranslator().css_to_xpath('div[aria-label="Reviews"]'))
sel_user_photo_count = XPath(GenericTranslator().css_to_xpath('div[aria-label="Photos"]'))

#Regexes
review_count_re = re.compile(r"(\d+)( reviews?)?")
rating_re = re.compile(r"(\d) star rating")
page_number_re = re.compile(r"(\d+) of (\d+)")
date_re = re.compile(r"(Updated \- )?(\d{1,2})/(\d{1,2})/(\d{4})")


#Not recommended reviews
#CSS selectors -- not recommended
sel_not_recommended_review_container = XPath(GenericTranslator().css_to_xpath("div.ysection.not-recommended-reviews.review-list-wide div.review.review--with-sidebar"))
sel_not_recommended_page_number = XPath(GenericTranslator().css_to_xpath("div.not-recommended-reviews div.page-of-pages"))
sel_not_recommended_review_count = XPath(GenericTranslator().css_to_xpath("div.ysection.not-recommended-reviews.review-list-wide > h3"))
sel_not_recommended_user_card = XPath(GenericTranslator().css_to_xpath("div.ypassport.media-block"))
sel_not_recommended_user_img = XPath(GenericTranslator().css_to_xpath("img.photo-box-img"))
sel_not_recommended_user_name = XPath(GenericTranslator().css_to_xpath("span.user-display-name"))
sel_not_recommended_user_location = XPath(GenericTranslator().css_to_xpath("li.user-location"))
sel_not_recommended_user_friends = XPath(GenericTranslator().css_to_xpath("li.friend-count > b"))
sel_not_recommended_user_reviews = XPath(GenericTranslator().css_to_xpath("li.review-count > b"))
sel_not_recommended_user_photos = XPath(GenericTranslator().css_to_xpath("li.photo-count > b"))
sel_not_recommended_rating = XPath(GenericTranslator().css_to_xpath("img[alt $= 'rating']"))
sel_not_recommended_no_rating = XPath(GenericTranslator().css_to_xpath("img[alt $= '(no rating)']"))
sel_not_recommended_date = XPath(GenericTranslator().css_to_xpath("span.rating-qualifier"))
sel_not_recommended_review_card = XPath(GenericTranslator().css_to_xpath("div.review-content"))
sel_not_recommended_content = XPath(GenericTranslator().css_to_xpath("p"))
sel_not_recommended_content_toggleable = XPath(GenericTranslator().css_to_xpath("span.js-content-toggleable.hidden"))

#Regexes
not_recommended_review_count_re = review_count_re
not_recommended_rating_re = re.compile(r"(\d).0 star rating")
not_recommended_page_number_re = re.compile("Page (\d+) of (\d+)")


#Removed reviews
sel_removed_reviews_root = XPath(GenericTranslator().css_to_xpath('div.removed-reviews'))
sel_removed_review_container = XPath(GenericTranslator().css_to_xpath("div.review.review--with-sidebar"))
sel_removed_page_number = XPath(GenericTranslator().css_to_xpath("div.page-of-pages"))
sel_removed_review_count = XPath(GenericTranslator().css_to_xpath("h3"))
sel_removed_user_card = sel_not_recommended_user_card
sel_removed_user_img = sel_not_recommended_user_img
sel_removed_user_name = sel_not_recommended_user_name 
sel_removed_user_location = sel_not_recommended_user_location 
sel_removed_user_friends = sel_not_recommended_user_friends 
sel_removed_user_reviews = sel_not_recommended_user_reviews 
sel_removed_user_photos = sel_not_recommended_user_photos 
sel_removed_rating = sel_not_recommended_rating 
sel_removed_no_rating = sel_not_recommended_no_rating
sel_removed_date = sel_not_recommended_date
sel_removed_review_card = sel_not_recommended_review_card 
sel_removed_content = sel_not_recommended_content 
sel_removed_content_toggleable = sel_not_recommended_content_toggleable 

#Regexes
removed_review_count_re = review_count_re
removed_rating_re = re.compile(r"(\d).0 star rating")
removed_page_number_re = re.compile("Page (\d+) of (\d+)")

@ratelimit.sleep_and_retry
@ratelimit.limits(calls=N_FETCH,period=FETCH_SLEEP * N_FETCH)
def extract_business_data(document):
    """
    Extracts business data. Currently just ammenities
    """
    if BANNED_TEXT in document:
        raise Exception("This address is banned from Yelp")
    
    root = etree.HTML(document)
    try:
        raw_data_container = sel_raw_data_container(root)[0]
    except:
        logger.error("Failed to get ammenities",exc_info=True)
        return {"ammenities_need_manual_intervention": True}
    data_str = etree.tostring(raw_data_container)
    raw_data = json.loads(data_str[data_str.index(rd_left_marker)+len(rd_left_marker):data_str.rindex(rd_right_marker)].replace(b"&amp;quot;",b"\""))
    
    #Extract the ammenities
    ammenities = []
    for k,v in raw_data.items():
        if ammenity_prefix in k:
            ammenities.append({ak: v[ak] for ak in ammenity_keys})
    
    return {"ammenities": ammenities, "ammenities_need_manual_intervention": False}

@ratelimit.sleep_and_retry
@ratelimit.limits(calls=N_FETCH,period=FETCH_SLEEP * N_FETCH)
def extract_recommended(document):
    """
    Extracts all the reviews and returns the total review count
    Largely traverses document tree through manually extracted features
    Probably needs to be updated every time Yelp updates their UI
    """
    if BANNED_TEXT in document:
        raise Exception("This address is banned from Yelp")
    
    root = etree.HTML(document)

    if "Hey there trendsetter! You could be the first review for" in root.xpath("string()"):
        logger.warning("No reviews")
        return {
            "reviews": [],
            "review_count": 0,
            "has_next": False,
            "page_number": 1,
            "page_count": 1
        }
    

    try:
        page_number_text = sel_page_number(root)[0].xpath("string()")
        page_count = int(page_number_re.match(page_number_text).group(2))
        page_number = int(page_number_re.match(page_number_text).group(1))
    except IndexError:
        logger.warning("Could not extract page number")
        page_count = 1
        page_number = 1
    


    has_next = bool(len(sel_next_page(root)) != 0) and page_number < page_count

    review_count = sel_review_count(root)[0].xpath("string()")
    review_count = int(review_count_re.match(review_count).group(1))
    if review_count <= 0:
        logger.warning("sel_review_count failed, falling back")
        review_count = sel_review_count_alt(root)[0].xpath("string()")
        review_count = int(review_count_re.match(review_count).group(1))
        
    assert review_count > 0
        

    #Grab all reviews

    review_list = None
    for elem in sel_review_container(root):
        if "Recommended Reviews" in elem.xpath("string()"):
            review_list = sel_review_list(elem)[0]

    assert review_list is not None

    review_accum = []

    #Iterate reviews
    for review in review_list:
        assert review.tag == "li"

        #Drop down one level
        review = review[0]
        assert review.tag == "div"

        #Grab user card
        user_card = review[0][0][0][0][0]

        #Account info
        name_and_loc = sel_user_card(review)[0]

        if "Qype User" in name_and_loc.xpath("string()"):
            logger.info("Skipping Qype user")
            continue

        #Detect if they are an elite user
        if len(sel_elite(user_card)) != 0:
            elite = True
        else:
            elite = False
        
        name = name_and_loc[0].xpath("string()")

        #There are 3 parts if user is elite
        if len(name_and_loc) == 2 if elite else 1:
            location = None
        else:
            location = name_and_loc[-1].xpath("string()")
            
        #stats = data_container[1]
        #stats_list = list(stats.iter("b"))

        #print(lxml.html.tostring(user_card))

        friends = sel_user_friend_count(user_card)[0].xpath("string()")
        reviews = sel_user_review_count(user_card)[0].xpath("string()")
        photo_container = sel_user_photo_count(user_card)
        if photo_container:
            photos = photo_container[0].xpath("string()")
        else:
            photos = "0"
            
        #Profile picture
        picture_container = user_card[0]
        user_image_url = list(picture_container.iter("img"))[0].get("src")
        user_page_url = list(picture_container.iter("a"))[0].get("href")

        #Grab the rating and review
        rating_date_div = review[1][0]
        
        rating = str(sel_rating(rating_date_div[0])[0])
        rating = int(rating_re.match(rating).group(1))
        #date = sel_date(rating_date_div)[0].xpath("string()")
        date = rating_date_div[1].xpath("string()")
        try:
            assert date_re.match(date)
        except:
            logger.error(f"Unable to match date {date}")
            raise

        review_lines = sel_review(review)[0].xpath("text()")
        review_lines = (line.strip() for line in review_lines)
        review = '\n'.join(review_lines)

        #Save the data
        #TODO grab date, review ID
        review_accum.append({
            "content": review,
            "rating": rating,
            "date": date,
            "user_image_url": user_image_url,
            "user_page_url": user_page_url,
            "user_name": name,
            "user_location": location,
            "user_friends": friends,
            "user_review_count": reviews,
            "user_photos": photos,
            "elite": elite
            })
    return {
        "reviews": review_accum,
        "review_count": review_count,
        "has_next": has_next,
        "page_number": page_number,
        "page_count": page_count
    }


@ratelimit.sleep_and_retry
@ratelimit.limits(calls=N_FETCH,period=FETCH_SLEEP * N_FETCH)
def extract_not_recommended(document):

    if BANNED_TEXT in document:
        raise Exception("This address is banned from Yelp")
    
    root = etree.HTML(document)
    
    review_count = sel_not_recommended_review_count(root)[0].xpath("string()")
    review_count = int(not_recommended_review_count_re.search(review_count).group(1))

    if review_count == 0:
        logger.info("Found no not recommended reviews")
        return {
            "reviews": [],
            "review_count": review_count,
            "page_number": 1,
            "page_count": 1
        }

    try:
        page_number_text = sel_not_recommended_page_number(root)[0].xpath("string()")
        page_count = int(not_recommended_page_number_re.search(page_number_text).group(2))
        page_number = int(not_recommended_page_number_re.search(page_number_text).group(1))
    except:
        logger.warning("Could not get page number",exc_info=True)
        page_count = 1
        page_number = 1
        

    review_accum = []

    #Iterate reviews
    for review in sel_not_recommended_review_container(root):

        #Grab user card
        user_card = sel_not_recommended_user_card(review)[0]

        #Need to skip Qype users
        if "Qype User" in user_card.xpath("string()"):
            logger.info("Skipping Qype user")
            continue
        
        #Account info
        name_container = sel_not_recommended_user_name(user_card)[0]

        #Profile picture
        picture_container = sel_not_recommended_user_img(user_card)[0]
        user_image_url = picture_container.get("src")
        
        name = name_container.xpath("string()").strip()
        hovercard_id = name_container.get("data-hovercard-id")
        try:
            location = sel_not_recommended_user_location(user_card)[0].xpath("string()").strip()
        except:
            location = None
        
        friends = sel_not_recommended_user_friends(user_card)[0].text
        reviews = sel_not_recommended_user_reviews(user_card)[0].text
        try:
            photos = sel_not_recommended_user_photos(user_card)[0].text
        except:
            photos = 0


        #Grab the rating and review
        review_card = sel_not_recommended_review_card(review)[0]

        #Rating and date
        try:
            rating = sel_not_recommended_rating(review_card)[0].get("alt")
            rating = int(not_recommended_rating_re.match(rating).group(1))
        except:
            logger.error("Couldn't get a rating, trying no rating")
            sel_not_recommended_no_rating(review_card)[0].get("alt") == "(no rating)"
            rating = None

            #This review isn't useful, skip it
            continue
            
        date = sel_not_recommended_date(review_card)[0].text.strip()
        assert date_re.match(date)

        #Review content
        review_container = None
        review_containers = sel_not_recommended_content_toggleable(review_card)
        
        review_containers_str = '\n\n\n'.join((e.xpath('string()') for e in review_containers))
        
        assert len(review_containers) <= 1
        if len(review_containers) == 1:
            review_container = review_containers[0]
        else:
            review_container = sel_not_recommended_content(review_card)[0]
        
        review = review_container.xpath("text()")
        review = (line.strip() for line in review)
        review = '\n'.join(review)

        if review.strip == "This review has been removed for violating our Terms of Service":
            continue

        #Save the data
        review_accum.append({
            "content": review,
            "rating": rating,
            "date": date,
            "user_image_url": user_image_url,
            "data_hovercard_id": hovercard_id,
            "user_name": name,
            "user_location": location,
            "user_friends": friends,
            "user_review_count": reviews,
            "user_photos": photos,
            })
    return {
        "reviews": review_accum,
        "review_count": review_count,
        "page_number": page_number,
        "page_count": page_count
    }
    
    
@ratelimit.sleep_and_retry
@ratelimit.limits(calls=N_FETCH,period=FETCH_SLEEP * N_FETCH)
def extract_removed(document):

    if BANNED_TEXT in document:
        raise Exception("This address is banned from Yelp")
    
    root = etree.HTML(document)

    try:
        root = sel_removed_reviews_root(root)[0]
    except IndexError as e:
        logger.info("Found no removed reviews")

        #We need to make sure we loaded the page correctly -- if we did, we should be able to extract not recommended reviews
        try:
            extract_not_recommended(document)
        except:
            logger.error("Couldn't get not recommended reviews, assuming failure")
            raise e
        
        return {
            "reviews": [],
            "review_count": 0,
            "page_number": 1,
            "page_count": 1
        }


    try:
        page_number_text = sel_removed_page_number(root)[0].xpath("string()")
        page_count = int(removed_page_number_re.search(page_number_text).group(2))
        page_number = int(removed_page_number_re.search(page_number_text).group(1))
    except:
        logger.warning("Could not get page number",exc_info=True)
        page_count = 1
        page_number = 1

    review_count = sel_removed_review_count(root)[0].xpath("string()")
    review_count = int(removed_review_count_re.search(review_count).group(1))

    review_accum = []

    #Iterate reviews
    for review in sel_removed_review_container(root):

        #Grab user card
        user_card = sel_removed_user_card(review)[0]

        #Need to skip Qype users
        if "Qype User" in user_card.xpath("string()"):
            logger.info("Skipping Qype user")
            continue
        
        #Account info
        name_container = sel_removed_user_name(user_card)[0]

        #Profile picture
        picture_container = sel_removed_user_img(user_card)[0]
        user_image_url = picture_container.get("src")
        
        name = name_container.xpath("string()").strip()
        hovercard_id = name_container.get("data-hovercard-id")
        try:
            location = sel_removed_user_location(user_card)[0].xpath("string()").strip()
        except:
            locaiton = None
        
        friends = sel_removed_user_friends(user_card)[0].text
        reviews = sel_removed_user_reviews(user_card)[0].text
        try:
            photos = sel_removed_user_photos(user_card)[0].text
        except:
            photos = 0


        #Grab the rating and review
        review_card = sel_removed_review_card(review)[0]

        #Rating and date
        try:
            rating = sel_removed_rating(review_card)[0].get("alt")
            rating = int(removed_rating_re.match(rating).group(1))
        except:
            logger.error("Couldn't get a rating, trying no rating")
            sel_removed_no_rating(review_card)[0].get("alt") == "(no rating)"
            rating = None

            #This review isn't useful, skip it
            continue
            
        date = sel_removed_date(review_card)[0].text.strip()
        assert date_re.match(date)

        #Review content
        review_container = None
        review_containers = sel_removed_content_toggleable(review_card)
        
        review_containers_str = '\n\n\n'.join((e.xpath('string()') for e in review_containers))
        
        assert len(review_containers) <= 1
        if len(review_containers) == 1:
            review_container = review_containers[0]
        else:
            review_container = sel_removed_content(review_card)[0]
        
        review = review_container.xpath("text()")
        review = (line.strip() for line in review)
        review = '\n'.join(review)

        if review.strip == "This review has been removed for violating our Terms of Service":
            continue

        #Save the data
        review_accum.append({
            "content": review,
            "rating": rating,
            "date": date,
            "user_image_url": user_image_url,
            "data_hovercard_id": hovercard_id,
            "user_name": name,
            "user_location": location,
            "user_friends": friends,
            "user_review_count": reviews,
            "user_photos": photos,
            })
    return {
        "reviews": review_accum,
        "review_count": review_count,
        "page_number": page_number,
        "page_count": page_count
    }

