#!/usr/bin/env python
# coding=utf-8
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import smtplib
import os

class BaseMail(object):

    def __init__(self, config, subject, content, receivers, filePath=""):
        self.config = config
        self.subject = subject
        self.receivers = receivers
        self.filePath = filePath
        self.content = content

    def sendMail(self):
        user = self.config.get("MAIL", "user")  # 发送邮件的邮箱地址
        sender = user  # 发送邮件的邮箱地址
        passwd = self.config.get("MAIL", "password")  # 发送邮件的邮箱密码
        smtpserver = self.config.get("MAIL", "smtp")  # 发送邮件的邮件服务器
        receiverlist = []  # 接收邮件的邮箱地址
        msgRoot = MIMEMultipart('related')
        if len(self.filePath) > 0 :
            baseName = os.path.basename(self.filePath)
            attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
            sendfile = open(self.filePath, 'rb').read()
            att = MIMEText(sendfile, 'plain', 'utf-8')
            att["Content-Type"] = "application/octet-stream"
            att["Content-Disposition"] = attachdisposition  # 发送附件html格式
            msgRoot.attach(att)  # 发送文件格式
        msgText = MIMEText(self.content, 'plain', 'utf-8')

        for addr in self.receivers:
            receiverlist.append(addr)
        receiver = ';'.join(receiverlist)
        msgRoot['Subject'] = self.subject
        msgRoot['From'] = sender
        msgRoot['To'] = receiver
        msgRoot.attach(msgText)  # 添加正文
        try:
            smtp = smtplib.SMTP_SSL(smtpserver, 465)
            smtp.login(user, passwd)
            smtp.sendmail(sender, self.receivers, msgRoot.as_string())
            print(datetime.now().strftime("%Y.%m.%d-%H:%M:%S"), '发送成功')
        except Exception as e:
            print(e)
        finally:
            #os.remove(self.filePath)
            smtp.quit()
