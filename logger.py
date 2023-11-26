import logging
from logging.handlers import RotatingFileHandler
import datetime

now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(now)

log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')

logFile = 'tool.log'

my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024, 
                                 backupCount=2, encoding=None, delay=0)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)

# 创建一个控制台handler，用于输出到控制台
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)


def get_logger(level=logging.INFO):
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    logger.addHandler(my_handler)
    logger.addHandler(console_handler)
    return logger