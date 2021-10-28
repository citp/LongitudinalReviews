sudo apt update
sudo apt install gcc g++ python3 python3-dev python3-venv pkg-config libicu-dev
#python3 -m venv venv
source venv/bin/activate

#Wheel needs to be installed first
pip install wheel
pip install --no-binary=:pyicu: pyicu
pip install -r requirements.txt
