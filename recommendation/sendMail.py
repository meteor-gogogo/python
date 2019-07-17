#!/usr/bin/python3
# coding=utf-8

import codecs
import smtplib
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
import sys

def sendMail(addr_list, file_name):
    msgRoot = MIMEMultipart()
    msgRoot.attach(MIMEText('hello, send by jumpsrv/data01...', 'plain', 'utf-8'))
    
    sender = 'aplumctr@aplum-inc.com'      # 发送邮件的邮箱地址
    receivers = ';'.join(addr_list)
    #print (receivers)
    msgRoot['From'] = sender
    msgRoot['To'] = receivers
    msgRoot['Subject'] = 'send file...'
    user = 'aplumctr@aplum-inc.com'      # 发送邮件的邮箱地址
    passwd = 'qZdCAf3MCDv8SXi'            # 发送邮件的邮箱密码
    smtp_server = 'smtp.aplum-inc.com'          # 发送邮件的邮件服务器

    #附件
    att = MIMEBase('application', 'octet-stream')
    att.set_payload(open(file_name, 'rb').read())
    encoders.encode_base64(att)
    att.add_header('Content-Disposition','attachment', filename=file_name)
    msgRoot.attach(att)

    server = smtplib.SMTP_SSL(smtp_server, 465)
    #server.set_debuglevel(1)
    server.login(user, passwd)
    server.sendmail(sender, receivers, msgRoot.as_string())
    server.quit()

'''
    filePath = '/home/aplum/inner/aplum_product_es/statistics/%s'%attachmentName
    baseName = os.path.basename(filePath)
    sendfile = open(filePath, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = "attachment;filename =%s"%baseName #发送附件html格式
    msgText = MIMEText('hello world', 'plain', 'utf-8')

    for addr in addressList:
        receivers.append(addr)
    msgRoot.attach(msgText) #添加正文
    msgRoot.attach(att) #发送文件格式
    #发送邮件
    try:
        smtp = smtplib.SMTP_SSL(smtpserver, 465)
        smtp.login(user, passwd)
        smtp.sendmail(sender, addressList, msgRoot.as_string())
        print datetime.now().strftime("%Y.%m.%d-%H:%M:%S"), '发送成功'
    except Exception as e:
        print(e)
    finally:
        os.remove(filePath)
        smtp.quit()
'''
if __name__ == '__main__':
    if len(sys.argv) <=1:
        print ("err: file is null")
        sys.exit(0)

    addr_list=[]
    for index in range(2,len(sys.argv)):
        addr_list.append(sys.argv[index])
    if len(addr_list) == 0:
        addr_list.append('liwenlong@aplum.com.cn')
     
    print('  send to: %s'%(','.join(addr_list)))
    print('send file: %s'%sys.argv[1])

    sendMail(addr_list, sys.argv[1])
 
   

