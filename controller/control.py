import flask
import atexit
import json
import collections
import datetime
import os
import pickle
import safer
import crawler.configuration as config


PAGE_INITIALIZE = "initialize"
PAGE_QUIT = "quit"
PAGE_CHECK_IN = "check_in"
PAGE_STATUS = "status"
PAGE_DELETE = "delete"

DATA_FN = "data/controller_status.json"
QUIT_FN = "config/quit"
QUIT_CRAWLER_FN = "config/quit_%s"

KEY = config.get_configuration(["controller","key"])

def recursive_dd_factory():
    return collections.defaultdict(recursive_dd_factory)
crawler_status = recursive_dd_factory()

@atexit.register
def save_data():
    if len(crawler_status) != 0:
        with safer.open(DATA_FN,"wb+",temp_file=True) as f:
            pickle.dump(crawler_status, f)
            
def recover_data():
    global crawler_status
    if not os.path.exists(DATA_FN):
        return
    
    with open(DATA_FN, "rb") as f:
        crawler_status = pickle.load(f)

def authenticate():
    if "key" in flask.request.args:
        key = flask.request.args["key"]
    elif "key" in flask.request.form:
        key = flask.request.form["key"]
    else:
        print("No key provided")
        return False

    if key != KEY:
        print(f"Key: {key}")
    
    return key == KEY

def update(action=None):
    crawler = flask.request.form["crawler_name"]
    
    if action == "initialize" and crawler in crawler_status:
        del crawler_status[crawler]
    
    
    data = {}
    for k in flask.request.form:
        if k == "crawler_name" or k == "key": continue
        data[k] = flask.request.form[k]
    data[f"time_{action}"] = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    
    for key in data:
        d = crawler_status[crawler]
        prev_d = None
        for v in key.split('_'):
            prev_d = d
            d = d[v]
        
        prev_d[v] = data[key]

    return data
    
def get_response(data):
    if os.path.exists(QUIT_FN) or ("crawler_name" in data and os.path.exists(QUIT_CRAWLER_FN % data["crawler_name"])):
        return {"quit": True}
    else:
        return {}
    
def setup():
    app = flask.Flask("Crawler Controller")
    
    recover_data()
       
    @app.route(f'/{PAGE_DELETE}', methods=['POST'])
    def delete():
        if not authenticate():
            return flask.jsonify(["Not authorized"])

        keys = flask.request.form["delete"].split(',')
        
        for key in keys:
            d = crawler_status
            prev_d = None
            for v in key.split('_'):
                prev_d = d
                d = d[v]
            
            del prev_d[v]
            
        
        return flask.jsonify(crawler_status)
    
    @app.route(f'/{PAGE_STATUS}', methods=['GET'])
    def status():
        if not authenticate():
            return flask.jsonify(["Not authorized"])
    
        now = datetime.datetime.now()
        del crawler_status["status"]
        crawler_status["now"] = now.strftime("%Y/%m/%d %H:%M:%S")
        for crawler_id in list(crawler_status):
            if crawler_id in ["status","now"]: #values used for other puproses
                continue
            if "quit" in crawler_status[crawler_id]["time"]:
                status = "terminated"
            elif "check-in" in crawler_status[crawler_id]["time"]:
                td = now - datetime.datetime.strptime(crawler_status[crawler_id]["time"]["check-in"], "%Y/%m/%d %H:%M:%S")
                status = str(td)
            else:
                status = "unknown"
            crawler_status["status"][crawler_id] = status
            
        return flask.jsonify(crawler_status)
             
    @app.route(f'/{PAGE_INITIALIZE}', methods=['POST'])
    def initialize():
        if not authenticate():
            return flask.jsonify(["Not authorized"])
        
        data = update(action="initialize")
        return flask.jsonify(get_response(data))
    
    @app.route(f'/{PAGE_CHECK_IN}', methods=['POST'])
    def check_in():
        if not authenticate():
            return flask.jsonify(["Not authorized"])
        
        data = update(action="check-in")
        return flask.jsonify(get_response(data))
    
    @app.route(f'/{PAGE_QUIT}', methods=['POST'])
    def quit():
        if not authenticate():
            return flask.jsonify(["Not authorized"])
        
        data = update(action="quit")
        return flask.jsonify(get_response(data))
        
    return app



app = setup()
if __name__ == "__main__":
    app.run(debug=True,host="0.0.0.0",ssl_context='adhoc')
    
