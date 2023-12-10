import os
import csv 
from common import *
from logger import get_logger

logger = get_logger()


# 读取一个list，追加到csv文件    
def write_error(error_list, err_file):
    # 如果文件不存在，创建文件并写入表头
    if not os.path.exists(err_file):
        with open(err_file, 'w', newline='', encoding=csv_encoding) as file:
            fieldnames = ['recipient', 'time','status', 'error']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(error_list)
    else:
        with open(err_file, 'a', newline='', encoding=csv_encoding) as file:
            fieldnames = ['recipient', 'time','status', 'error']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writerows(error_list)

# 读取一个list，根据条件修改csv文件
# 遍历List, 如果元素的某个属性满足条件，修改csv文件中的某个属性
def write_success(success_list, log_file):
    if not os.path.exists(log_file):
        with open(log_file, 'w', newline='', encoding=csv_encoding) as file:
            fieldnames = ['recipient', 'time','status', 'error']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            rows = []
    else:
        with open(log_file, 'r', encoding=csv_encoding) as file:
            reader = csv.DictReader(file)
            rows = [row for row in reader]
        
    for row in rows:
        for success in success_list:
            if row['recipient'] == success['recipient']:
                row['time'] = success['time']
                row['status'] = 'Success'
                
    with open(log_file, 'w', newline='', encoding=csv_encoding) as file:
        fieldnames = ['recipient', 'time', 'status']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
    logger.info(f'Log file updated: {log_file}')

    