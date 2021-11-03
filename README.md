# Collection and analysis of longitudinal reviews on Yelp

This project is a repository containing code

## Data

Our data has not yet been released, but the announcement page is [here](https://sites.google.com/princeton.edu/longitudinal-review-data/home).

## Setup

You will need to enter the relevant fields into the config/config.json file

You will need to put your VPN credentials in vpn_credentials.json and vpn_credentials.txt

You will need to put Open VPN .ovpn configuration files for each possible VPN configuration in ovpn_files/

You will need to put your Yelp API credentials into yelp_api_credentials.json

## Crawler

First you will need to get a target set of businesses. crawler.find_business can help here -- if you provide a sequence of zipcodes, it will collect them. You may need to repeat this process several times due to API query limits

Next, you'll need to deploy the crawler.

First, set up the control server:
- Install gunicorn
- Generate a self-signed SSL certificate
- Ensure you have an SSH server is open
- Install the python requirements
- Run scripts/run_controller.sh

Next set up the crawler
- Ensure you have SSH access to the control sever from the crawler
- Put the relevant certificate files into certificates/
- Install the python requirements
- Run scripts/setup_reverse_tunnel.sh (this step is important! you may otherwise lose control of the machine once the VPN starts up. Make sure each crawler has their own port)
- Run scripts/start_crawler

You will most likely need to debug the review parser, as it needs to be updated when Yelp adjusts their site. Check crawler.review_parser. You may also need to adjust the crawler in crawler.crawl_reviews

You can monitor your crawlers from /status?key=<key>

When finished with the second or later crawl, crawler.checks can check the review retention as a baseline check

## Analysis
All analysis is done through Jupyter notebooks

Start the notebook server: ./scripts/run_notebook.sh
