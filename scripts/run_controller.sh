#Needs to be run as sudoer
source venv/bin/activate
python -m gunicorn --certfile certificates/cert.pem --keyfile keys/key.pem -b 0.0.0.0:443 controller.control:app
