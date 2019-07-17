#!/usr/bin/env python
# coding=utf-8
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import smtplib
import os


class BaseMail(object):
    def __init__(self, config, subject, content, receivers, file_path=""):
        self.config = config
        self.subject = subject
        self.content = content
        self.receivers = receivers
        self.file_path = file_path

    def send_mail(self):
        # 发送邮件的邮箱地址
        sender = self.config.get("MAIL", "user")
        # 发送邮件的邮箱密码
        password = self.config.get("MAIL", "password")
        # 发送邮件的邮件服务器
        smtpserver = self.config.get("MAIL", "smtp")
        # 定义接收邮件的邮箱地址列表
        receiver_list = list()
        # 构造MIMEMultipart对象为根容器，MIMEMultipart的类型信息，主要有三种子类型：mixed,alternative,related
        # 详细信息，请参照：https://blog.csdn.net/Winnycatty/article/details/84548381，https://www.cnblogs.com/alaska1131/articles/1852653.html
        main_msg = MIMEMultipart('related')
        # 判断文件路径是否合法
        if len(self.file_path) > 0:
            # 获取路径中的文件名
            base_name = os.path.basename(self.file_path)
            attach_disposition = "attachment:filename = %s" % base_name
            # 最好不用下边的写法，因为资源没法关闭
            # send_file = open(self.file_path, 'rb').read()
            # 替换为以下写法
            data = open(self.file_path, 'rb')
            send_file = data.read()
            data.close()
            attach = MIMEText(send_file, 'plain', 'utf-8')
            attach['Content-Type'] = "application/octet-stream"
            attach['Content-Disposition'] = attach_disposition
            main_msg.attach(attach)
        msg_text = MIMEText(self.content, 'plain', 'utf-8')

        for receiver in receiver_list:
            receiver_list.append(receiver)

        receiver_with_delimiter = ';'.join(receiver_list)
        main_msg['Subject'] = self.subject
        main_msg['From'] = sender
        main_msg['To'] = receiver_with_delimiter
        main_msg.attach(msg_text)

        try:
            smtp = smtplib.SMTP_SSL(smtpserver, 465)
            smtp.login(sender, password)
            smtp.sendmail(sender, self.receivers, main_msg.as_string())
            print(datetime.now().strftime("%Y-%m-%d %H-%M-%S"), '发送成功')
        except Exception as e:
            print(str(e))
        finally:
            smtp.quit()
