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
account_path = os.path.join(os.path.dirname(__file__), 'conf', 'account.csv')
recipients_path = os.path.join(os.path.dirname(__file__), 'conf', 'recipients.csv')
email_path = os.path.join(os.path.dirname(__file__), 'conf', 'email.csv')
log_file_path = os.path.join(os.path.dirname(__file__), 'conf', 'log.csv')

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
        

    def send_email(self, to, subject, body, receiver_name=None, subject_raw=None):
        # 设置发件人和SMTP服务器信息
       
        sender_alias = self.alias
        sender_email = self.user


        # 创建邮件对象
        msg = EmailMessage()
        msg.add_header('From', f'{sender_alias} <{sender_email}>')
        msg['To'] = to
        if receiver_name is not None and '{name}' in subject:
            msg['Subject'] = subject.replace('{name}', receiver_name)
        else:
            msg['Subject'] = subject_raw

        # 添加邮件正文
        msg.set_content(body, subtype='html', charset='utf-8')


        try:
            self.server.send_message(msg)
        except Exception as e:
            logger.error(f'Error: {str(e)}')
            raise e
            


def do_send(email_sender, recipient, subject, body, log_file, subject_raw=None):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        logger.info(f'Sending to {recipient}')
        receiver_email, receiver_name = recipient
        email_sender.send_email(receiver_email, subject, body, receiver_name, subject_raw)
        
        write_error([{'recipient': receiver_email, 'time': now, 'status':'Success', 'error': None}], log_file)
        logger.info(f'Send to {recipient} Success')
    except Exception as e:
        logger.error(f'Send to {recipient} Fails.', e)
        # 记录发送失败的邮件
        write_error([{'recipient': receiver_email, 'time': now, 'status':'Fail','error': str(e)}], log_file)

def worker(task_queue, sender, subject, email_body, log_file, subject_raw=None):
    while True and not task_queue.empty():
        logger.info('begin to work')
        item = task_queue.get() # wait for job
        logger.info(f'get work: {item}')
        do_send(sender, item, subject, email_body, log_file, subject_raw )
        task_queue.task_done()
        logger.info(f'task done')


def check_csv_exists(file_path, file_name, fieldnames):
    if not os.path.exists(file_path):
        logger.error(f'{file_name} 配置不存在，已创建初始化配置文件：{file_path}')
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
        raise Exception(f'请补充{file_name}到文件：{file_path}')
    
def read_csv_dict(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8')  as file:
            reader = csv.DictReader(file)
            return next(reader)
    raise Exception(f'文件不存在 {file_path}')

def read_csv_dict_list(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8')  as file:
            reader = csv.DictReader(file)
            return [row for row in reader]
    raise Exception(f'文件不存在 {file_path}')

def check_email_config(email_config):
    logger.info(f'邮件配置 {email_config}')
    body_file = email_config.get('body_file') 
    if body_file is None or not os.path.exists(body_file):
        raise Exception(f'请检查邮件配置，body_file {body_file} 不存在')
    subject = email_config.get('subject')
    if check_str_blank(subject):
        raise Exception(f'请检查邮件配置，subject 不存在或者为空')
    subject_raw = email_config.get('subject_raw')
    if check_str_blank(subject_raw):
        raise Exception(f'请检查邮件配置，subject_raw 不存在或者为空')
    return subject, subject_raw, body_file

def check_str_blank(strs):
    return strs is None or len(strs) == 0

def check_smtp_config(smtp_config):
    host = smtp_config.get('host')
    port = smtp_config.get('port')
    alias = smtp_config.get('alias')
    user = smtp_config.get('user')
    password = smtp_config.get('password')
    if check_str_blank(host):
        raise Exception(f'host 为空')
    if check_str_blank(port):
        raise Exception(f'port 为空')
    if check_str_blank(alias):
        raise Exception(f'alias 为空')
    if check_str_blank(user):
        raise Exception(f'user 为空')
    if check_str_blank(password):
        raise Exception(f'password 为空')
    return True

def main():

    # 读取收件人配置
    check_csv_exists(recipients_path, '收件人', ['recipient', 'name'])
    receivers = read_csv_dict_list(recipients_path)
    recipients = [(row.get('recipient'), row.get('name'))  for row in receivers]

    # 读取email配置
    check_csv_exists(email_path, '邮件信息', ['subject','subject_raw','body_file'])
    email_config = read_csv_dict(email_path)
    subject, subject_raw, html_file = check_email_config(email_config)

    # 读取日志配置
    check_csv_exists(log_file_path, '发送日志', ['recipient', 'time','status', 'error'])
    
    # 读取发件人配置
    check_csv_exists(account_path, '发件人', ['host', 'port', 'alias', 'user', 'password'])
    smtp_list = read_csv_dict_list(account_path)

    senders = [EmailSender(smtp.get('host'), smtp.get('port'), 
                           smtp.get('user'), smtp.get('password'), 
                           smtp.get('alias')) for smtp in smtp_list if check_smtp_config(smtp)]

   
    # 检查log文件，如果已经发送成功，不再发送
    
    with open(log_file_path, 'r') as file:
        reader = csv.DictReader(file)
        sent_list = [row['recipient'] for row in reader if row['status'] == 'Success']
    
    # 去除已经发送成功的邮件
    recipients = [recipient for recipient in recipients if recipient[0] not in sent_list]
    
    
    with open(html_file, 'r', encoding='utf-8') as file:
        email_body = file.read()
        
    # 逐个发送邮件
    total_queue = queue.Queue()
    for recipient in recipients:
        total_queue.put(recipient)
    
    with concurrent.futures.ThreadPoolExecutor(thread_name_prefix="sender") as pool:
        for sender in senders:
            pool.submit(worker, total_queue, sender, subject, email_body, log_file_path, subject_raw)

    total_queue.join()
    for sender in senders:
        sender.destroy_smtp()
    logger.info('Email sending completed')

    
    
# 读取一个list，追加到csv文件    
def write_error(error_list, err_file):
    # 如果文件不存在，创建文件并写入表头
    if not os.path.exists(err_file):
        with open(err_file, 'w', newline='') as file:
            fieldnames = ['recipient', 'time','status', 'error']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(error_list)
    else:
        with open(err_file, 'a', newline='') as file:
            fieldnames = ['recipient', 'time','status', 'error']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writerows(error_list)

# 读取一个list，根据条件修改csv文件
# 遍历List, 如果元素的某个属性满足条件，修改csv文件中的某个属性
def write_success(success_list, log_file):
    if not os.path.exists(log_file):
        with open(log_file, 'w', newline='') as file:
            fieldnames = ['recipient', 'time','status', 'error']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            rows = []
    else:
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
    try:
        main()
    except Exception as e:
        logger.error(f'遇到了一点问题，程序异常退出', e)
    # 接收控制台输入，防止程序退出
    input('Press Enter to exit...')
