# coding:utf-8

'''
Created on Nov 23, 2012

@author: Eason
'''
__config__ = 'monitor.ini'

import sys
import os

import ConfigParser
import re
import datetime
import smtplib
from email.mime.text import   MIMEText
from email.mime.multipart import MIMEMultipart

from public import db_slave

class Common(object):

    def __init__(self):
        ConfigParser.RawConfigParser.OPTCRE = re.compile(r'(?P<option>[^=\s][^=]*)\s*(?P<vi>[=])\s*(?P<value>.*)$')
        self.CONFIG = ConfigParser.ConfigParser()
        self.CONFIG.read(os.path.join(os.path.dirname(__file__), __config__))
        self.check_keys = []
        self.addresses = []
        if self.CONFIG.has_section('notify_recevers'):
            self.addresses = self.CONFIG.get('notify_recevers', 'email').split(',')
        if self.CONFIG.has_section('mongo_daily_target'):
            check_keys = self.CONFIG.get('mongo_daily_target', 'check_keys').split(',')
            print check_keys
            for ck in check_keys:
                check_target = {}
                #check_target['key'] = ck
                if not self.CONFIG.has_section(ck):
                    print u'找不到指定的key:%s,请确认配置是否完整' % ck
                    continue
                check_target['key'] = self.CONFIG.get(ck, 'key')
                check_target['check_type'] = self.CONFIG.get(ck, 'check_type')
                check_target['check_interval_days'] = self.CONFIG.get(ck, 'check_interval_days')
                check_target['check_date_fmt'] = self.CONFIG.get(ck, 'check_date_fmt')
                self.check_keys.append(check_target)
                
                
    
class Checker(object):
    
    has_error = False
    
    error_content = ''
    
    @classmethod
    def check_created(cls, key, date, interval_days=1, check_date_fmt='%Y%m%d' , **args):
        exists = False
        from_date = date - datetime.timedelta(days=interval_days)

        
        cls.error_content += 'data key: <b>%s</b>  check_type:%s\n <br>'  % (key, '是否生成数据')
        for i in range(1, interval_days + 1):
            start_date = from_date + datetime.timedelta(days=i)
            date_str = start_date.strftime(check_date_fmt)
            cond = {'key':key, 'date':date_str}
            if db_slave.reports.find_one(cond):
                result = '<font color="grenn">OK</font><br>'
            else:
                result = '<font color="red">NG</font><br>'
                cls.has_error = True
            cls.error_content += '    report date:%s result:%s\n' % (date_str, result) 
            #print cond
        
        
        cls.error_content += '<br><br><br><hr>\n'
        return exists
    
def check_all_keys(keys):
    yesterday = datetime.datetime(*datetime.datetime.now().timetuple()[:3]) - datetime.timedelta(days=1)
    yesterday
    for key in keys:
        report_key = key['key']
        check_type = key['check_type']
        check_interval_days = key['check_interval_days']
        if 'check_date_fmt' in key:
            check_date_fmt = key['check_date_fmt']
        else:
            check_date_fmt = '%Y%m%d'
        check_method_to_call = getattr(Checker, 'check_' + check_type)
        check_method_to_call(report_key, yesterday, int(check_interval_days), check_date_fmt)
        
    
    return 'TODO'


HTML_TEMPLATE = '''\
    <html>
        <head>
            <title></title>
        <head>
        <body>
            ${body}
        </body>
    </html>
'''

def notify_users(addresses, title, content):
    #print title
    #print content
    if Checker.has_error :
        title += 'Important：有错误'
    
    result = HTML_TEMPLATE.replace('${body}', content)
    msg = MIMEMultipart('alternative')
    part1 = MIMEText(result, 'plain')
    part2 = MIMEText(result, 'html')
    msg['Subject'] = title
    msg['From'] = 'jiepang.dashboard@gmail.com'
    msg['To'] = addresses[0]
    msg.attach(part1)
    msg.attach(part2)
    #s = smtplib.SMTP('localhost')
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login('jiepang.dashboard@gmail.com', 'temptemp1')
    #s.connect()
    print result
    s.sendmail('jiepang.dashboard@gmail.com', addresses, msg.as_string())
    s.close()
    
def main():
    common = Common()
    #print common.check_keys
    check_all_keys(common.check_keys)
    if Checker.error_content:
        notify_users(common.addresses, '后台数据统计监测报告', Checker.error_content)

if __name__ == '__main__':
    
    main()
    
