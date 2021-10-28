"""
All known categories for Yelp restaurants. Used in find_business
"""

import itertools
import random

categories = [
    "afghani","african","newamerican","tradamerican","andalusian","arabian","argentine",
    "armenian","asianfusion","asturian","australian","austrian","baguettes",
    "bangladeshi","bbq","basque","bavarian","beergarden","beerhall","beisl",
    "belgian","bistros","blacksea","brasseries","brazilian","breakfast_brunch",
    "british","buffets","bulgarian","burgers","burmese","cafes","cafeteria",
    "cajun","cambodian","newcanadian","canteen","caribbean","catalan","cheesesteaks",
    "chickenshop","chicken_wings","chilean","chinese","comfortfood","corsican",
    "creperies","cuban","currysausage","cypriot","czech","czechslovakian","danish",
    "delis","diners","dinnertheater","dumplings","eastern_european","eritrean",
    "ethiopian","hotdogs","filipino","fischbroetchen","fishnchips","flatbread",
    "fondue","food_court","foodstands","freiduria","french","sud_ouest","galician",
    "gamemeat","gastropubs","georgian","german","giblets","gluten_free","greek",
    "guamanian","halal","hawaiian","heuriger","himalayan","honduran","hkcafe","hotdog",
    "hotpot","hungarian","iberian","indpak","indonesian","international","irish",
    "island_pub","israeli","italian","japanese","jewish","kebab","kopitiam","korean",
    "kosher","kurdish","laos","laotian","latin","raw_food","lyonnais","malaysian",
    "meatballs","mediterranean","mexican","mideastern","milkbars","modern_australian",
    "modern_european","mongolian","moroccan","newmexican","newzealand","nicaraguan",
    "nightfood","nikkei","noodles","norcinerie","opensandwiches","oriental",
    "pfcomercial","pakistani","panasian","eltern_cafes","parma","persian","peruvian",
    "pita","pizza","polish","polynesian","popuprestaurants","portuguese","potatoes",
    "poutineries","pubfood","riceshop","romanian","rotisserie_chicken","russian",
    "salad","sandwiches","scandinavian","schnitzel","scottish","seafood",
    "serbocroatian","signature_cuisine","singaporean","slovakian","somali","soulfood",
    "soup","southern","spanish","srilankan","steak","supperclubs","sushi","swabian",
    "swedish","swissfood","syrian","tabernas","taiwanese","tapas","tapasmallplates",
    "tavolacalda","tex-mex","thai","norwegian","traditional_swedish","trattorie",
    "turkish","ukrainian","uzbek","vegan","vegetarian","venison","vietnamese",
    "waffles","wok","wraps","yugoslav"
]

#Adapted from https://docs.python.org/3/library/itertools.html#itertools-recipes
def grouper(iterable, n):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    chunks = itertools.zip_longest(*args, fillvalue=None)
    for chunk in chunks:
        #Get the highest non-empty value
        i = max(j for j in range(len(chunk)) if chunk[j] != None)
        chunk = chunk[:i+1]
        yield chunk

def get_grouped_categories(group_size):
    categories_rand = random.sample(categories,len(categories))
    for group in grouper(categories_rand,group_size):
        yield ",".join(group)
