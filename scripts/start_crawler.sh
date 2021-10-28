crawl_id="$1"
instance_id="$2"

if [ ! $# -eq 2 ]
  then
      echo "./$0 <crawl_id> <instance_id>"
      exit
fi

git pull

./scripts/start_tunnel_session.sh "config/port_${instance_id}.txt"

tmux new -d -s crawler
tmux send-keys -t crawler.0 "export REQUESTS_CA_BUNDLE=$(pwd)/certificates/cert.pem; cd $(pwd); source venv/bin/activate; python -m crawler.crawl_reviews --yelp_zip --zipcode_file config/crawl_${instance_id}.txt --crawl_id crawl_${crawl_id} --crawler_name ${instance_id}; sudo killall openvpn" ENTER
