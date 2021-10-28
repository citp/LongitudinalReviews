import argparse
import json

CONFIG_FILE = "config/config.json"
config = None

def get_configuration(keys,config_file=CONFIG_FILE):
    global config
    
    if config is None:
        with open(config_file) as fp:
            config = json.load(fp)
            
    
    obj = config
    for key in keys:
        obj = obj[key]

    return obj

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get a configuration value')
    parser.add_argument('keys', nargs='+',
                    help='Keys for configuration')
    parser.add_argument('--config', default=CONFIG_FILE,
                    help='Configuration file')



    args = parser.parse_args()
        
    print(get_configuration(args.keys,args.config),end='')
        
    
