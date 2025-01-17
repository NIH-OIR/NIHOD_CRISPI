import yaml

def load_config_file():
    conf = yaml.load(open("conf.yaml"), Loader = yaml.FullLoader)
    return conf