import csv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage

import ssl
from logger import get_logger
from config import YamlConfig
import datetime
import time
import os
import concurrent.futures
import queue

logger = get_logger()
config_path = os.path.join(os.path.dirname(__file__), 'conf', 'task.yaml')
config = YamlConfig(config_path)

class EmailSender:
    
    def __init__(self, host, port, user, password, alias):
        self.host= host
        self.port= port
        self.user = user
        self.password = password
        self.alias = alias
        logger.info(f'Initializing SMTP server {self.host}:{self.port} {self.user}')
        self.init_smtp()
        
        
    def init_smtp(self):
        context = ssl.create_default_context()
        context.set_ciphers('DEFAULT')
        
        self.server = smtplib.SMTP_SSL(self.host, self.port, context=context)
        self.server.login(self.user, self.password)
        
        
    def destroy_smtp(self):
        if self.server is not None:
            self.server.quit()
            self.server = None
            logger.info('SMTP server destroyed')
        

    def send_email(self, to, subject, body):
        # 设置发件人和SMTP服务器信息
       
        sender_alias = self.alias
        sender_email = self.user


        # 创建邮件对象
        msg = EmailMessage()
        msg.add_header('From', f'{sender_alias} <{sender_email}>')
        msg['To'] = to
        msg['Subject'] = subject

        # 添加邮件正文
        msg.set_content(body, subtype='html', charset='utf-8')


        try:
            self.server.send_message(msg)
        except Exception as e:
            logger.error(f'Error: {str(e)}')
            raise e
            


def do_send(email_sender, recipient, subject, body, log_file):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        logger.info(f'Sending to {recipient}')

        email_sender.send_email(recipient, subject, body)
        
        write_error([{'recipient': recipient, 'time': now, 'status':'Success', 'error': None}], log_file)
        logger.info(f'Send to {recipient} Success')
    except Exception as e:
        logger.error(f'Send to {recipient} Fails.', e)
        # 记录发送失败的邮件
        write_error([{'recipient': recipient, 'time': now, 'status':'Fail','error': str(e)}], log_file)

def worker(task_queue, sender, subject, email_body, log_file):
    while True and not task_queue.empty():
        logger.info('begin to work')
        item = task_queue.get() # wait for job
        logger.info(f'get work: {item}')
        do_send(sender, item, subject, email_body, log_file )
        task_queue.task_done()
        logger.info(f'task done')


def main():
    csv_file = config.get_or_default('recipient','address_book', default_value='conf/recepients.csv') 
    html_file = config.get_or_default('email','body_file', default_value='conf/email_body.html')
    log_file = config.get_or_default('recipient','log_file', default_value='conf/log.csv')
    
    subject = config.get_or_default('email','subject', default_value='Test Email')
    
    smtp_list = config.get_list('smtp')

    senders = [EmailSender(smtp.get('host'), smtp.get('port'), 
                           smtp.get('user'), smtp.get('password'), 
                           smtp.get('alias')) for smtp in smtp_list]

    # 读取CSV文件
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        recipients = [row['recipient'] for row in reader]
    with open(log_file, 'r') as file:
        reader = csv.DictReader(file)
        sent_list = [row['recipient'] for row in reader if row['status'] == 'Success']
    
    recipients = [recipient for recipient in recipients if recipient not in sent_list]
    with open(html_file, 'r', encoding='utf-8') as file:
        email_body = file.read()
        
    # 逐个发送邮件
    total_queue = queue.Queue()
    for recipient in recipients:
        total_queue.put(recipient)
    
    with concurrent.futures.ThreadPoolExecutor(thread_name_prefix="sender") as pool:
        for sender in senders:
            pool.submit(worker, total_queue, sender, subject, email_body, log_file)

    total_queue.join()
    for sender in senders:
        sender.destroy_smtp()
    logger.info('Email sending completed')

    
    
# 读取一个list，追加到csv文件    
def write_error(error_list, err_file):
    with open(err_file, 'a', newline='') as file:
        fieldnames = ['recipient', 'time','status', 'error']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writerows(error_list)

# 读取一个list，根据条件修改csv文件
# 遍历List, 如果元素的某个属性满足条件，修改csv文件中的某个属性
def write_success(success_list, log_file):
    with open(log_file, 'r') as file:
        reader = csv.DictReader(file)
        rows = [row for row in reader]
        
    for row in rows:
        for success in success_list:
            if row['recipient'] == success['recipient']:
                row['time'] = success['time']
                row['status'] = 'Success'
                
    with open(log_file, 'w', newline='') as file:
        fieldnames = ['recipient', 'time', 'status']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
    logger.info(f'Log file updated: {log_file}')

    

if __name__ == '__main__':
    main()
    # 接收控制台输入，防止程序退出
    input('Press Enter to exit...')
