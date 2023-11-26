#encoding=utf-8
import yaml
from logger import get_logger

logger = get_logger()

yaml_config = 'conf/task.yaml'

class YamlConfig:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.config = None
        self.load_config()
        
    def load_config(self):
        logger.info(f'Loading config from {self.yaml_file}')
        with open(self.yaml_file, 'r') as f:
            self.config = yaml.safe_load(f)
        logger.info(f'Config loaded success: {yaml_config}')
        
    def get_or_default(self, *args,default_value=None):
        conf = dict(self.config)
        for key in args:
            try:
                conf = conf[key]
            except KeyError:
                logger.error(f'KeyError: {key}')
                return default_value
        return conf
    
    def get_list(self, *args):
        conf = self.get_or_default(*args, default_value=[])
        if isinstance(conf, list):
            return conf
        else:
            return [conf]    

config = YamlConfig(yaml_config)


if __name__ == '__main__':
    logger.info('Testing config.py')
    logger.info(config.get_or_default('recipient', 'address_book'))
    logger.info(config.get_list('smtp','credential'))