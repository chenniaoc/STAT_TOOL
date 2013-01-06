# -*- coding:utf-8 -*-
import config
config.DB_CONNECT_STRING = 'mysql://jiepang:j1epan9_w0za1@192.168.10.10:3300/wozai_lbs?charset=utf8'


from public import *
from tools.stat.utils import get_id
import pickle


import datetime
from common import stat_util

from common.csv_writer import CommonCsvWriter


def measure_checkin_days(uid , max_days = 30):
    days = set()
    for p in crab.user_post[uid].find().sort(-R.created_on):
        c = p['created_on']
        created_date = datetime.datetime.fromtimestamp(c)
        days.add(created_date.strftime('%Y%m%d'))
        if len(days) >= max_days:
            return True
    else:
        return False
    


def populate_users_posts(ds, de ,all_users):
    #ds, de is utc
    start_id = get_id('posts','created_on', ds)
    end_id = get_id('posts', 'created_on', de)
    print start_id, end_id
    start_id -= 1
    while 1:
        print start_id
        count = db_slave.post.find({'_id': {'$gt': start_id, '$lt': end_id}}).limit(1).count(True)
        if count == 0:
            break
        for r in db_slave.post.find({'_id': {'$gt': start_id, '$lt': end_id}},{'del':1,'c':1,'nc':1,'nl':1,'l':1,'u':1}).sort('_id',1).limit(10000):
            #if r['t'] in [1,2,3,5,7]:
                #temp = all_posts.get(r['u']) or []
                #temp.append(r)
                #all_posts[r['u']] = temp
            start_id = r['_id']
            all_users.add(r['u'])

DUMP_FILE_PATH = 'exclude_users.dat'
def load_from_file():
    global exclude_users
    print 'start : load_from_file'
    try:
        exclude_users = pickle.load(open(DUMP_FILE_PATH, 'rb'))
    except:
        print 'failed : load_from_file'
        return None

def dump_to_file():
    global exclude_users
    print 'start : dump_to_file'
    try:
        output = open(DUMP_FILE_PATH, 'wb')
        pickle.dump(exclude_users, output)
    except:
        print 'failed : dump_to_file'
        return None
    
exclude_users = set()
def exclude_user(uid):
    start_date = datetime.datetime(2012,11,11)  - datetime.timedelta(hours = 8)
    end_date = datetime.datetime(2012,12,12)  - datetime.timedelta(hours = 8)
    #start_date = stat_util.convert_datetime_to_timestamp(start_date)
    #end_date = stat_util.convert_datetime_to_timestamp(end_date)
    global exclude_users
    if not exclude_users:
        load_from_file()
        if not exclude_users:
            populate_users_posts(start_date,end_date,exclude_users)
            dump_to_file()
    #return crab.user_post[uid].find((R.created_on >= start_date) & (R.created_on < end_date)).count()  == 0
    return uid not in exclude_users
    

def get_target_users_by_month(month):
    start_date = datetime.datetime(2012,month,1) - datetime.timedelta(hours = 8)
    if month == 12:
        end_date =  datetime.datetime(2013,1,1) - datetime.timedelta(hours = 8)
    else:
        end_date =  datetime.datetime(2012,(month + 1),1) - datetime.timedelta(hours = 8)
    users = []
    while True:
        #print 'User.query.filter((User.created_on >= start_date) & (User.created_on < end_date)).count()' , \
        #                User.query.filter((User.created_on > start_date) & (User.created_on < end_date)).count(),\
        #                    start_date , end_date
        if User.query.filter((User.created_on > start_date) & (User.created_on < end_date)).count() == 0:
            break
        
        for user in User.query.filter((User.created_on > start_date) & (User.created_on < end_date)).order_by(User.created_on).limit(5000).all():
            
            if (user.client_type.upper().find(u'J2ME') != -1 or user.client_type.upper().find(u'TOUCH') != -1 or user.client_type.upper().find(u'IPHONE') != -1 or user.client_type.upper().find(u'ANDROID') != -1  or user.client_type.upper().find(u'WP') != -1 or user.client_type.upper().find(u'NOKIA') != -1 or user.client_type.upper().find(u'100423') != -1  or user.client_type.upper().find(u'VERTU') != -1) and user.nick.find(u'已封禁') == -1 and not user.name.startswith('+'):
                if exclude_user(user.id) and measure_checkin_days(user.id) :
                    users.append(user)
            
            start_date = user.created_on
        print 'hinted user:',len(users)
        
    return users
import os
def populate_DAU(ds, de, logs):
    #ds, de is utc
    while ds < de:
        date = ds 
        with open('/home/jiepang/.logger/aru/activate_user-%s.csv' % str(date.date())) as f:
            us = set()
            for id in f:
                try:
                    user_id = int(id.rstrip(os.linesep))
                    us.add(user_id)
                except:
                    print 'userid error type,',id
            logs[date.date()] = us
        ds += datetime.timedelta(days=1)
        
        
def get_friends_and_counts(user_id, date):
    #date is bj
    c_active = 0
    c_total = []
    for r in crab.user_friend[user_id].find(R.created_on < calendar.timegm((date - datetime.timedelta(hours=8) + datetime.timedelta(days=1)).timetuple())):
        c_total.append(r['user_id'])
    return c_total

def is_readactivity(uid,start_month,end_month,log):
    for day in range(0,(end_month - start_month).days):
        start = start_month +  datetime.timedelta(days = day)
        monthly_dau = log.get(start.date()) or set()
        if uid in monthly_dau:
            return True
        
    return False

def is_writeactivity(uid,start_month,end_month):
    start = start_month - datetime.timedelta(hours = 8)
    end = end_month - datetime.timedelta(hours = 8)
    start = stat_util.convert_datetime_to_timestamp(start)
    end = stat_util.convert_datetime_to_timestamp(end)
    return crab.user_post[uid].find((R.created_on >= start) & (R.created_on <end).count()) > 0

def export_data():
    DAU_LOG = {}
    populate_DAU(datetime.datetime(2012,2,20),datetime.datetime(2012,11,1),DAU_LOG)
    
    csw = CommonCsvWriter('stat_HARDCODE-1024')
    csw.write_header([u'注册月份',u'追踪月份',u'has_read',u'has_write',u'friends count','feed'])
    for month in range(1,13):
        target_users = get_target_users_by_month(month)
        for m in range(3,11):
            start_month =  datetime.datetime(2012,m,1)
            end_month = datetime.datetime(2012,(m + 1),1)
            start_timestamp = stat_util.convert_datetime_to_timestamp(start_month)
            end_timestamp = stat_util.convert_datetime_to_timestamp(end_month)
            last_day_of_month = end_month - datetime.timedelta(days = 1,hours = 8) 
            for user in target_users:
                uid = user.id
                is_read_activtity = 'YES' if is_readactivity(uid,start_month,end_month,DAU_LOG) else 'NO'
                is_write_activity = 'YES' if is_writeactivity(uid,start_month,end_month) else 'NO'
                friends = get_friends_and_counts(uid,last_day_of_month)
                seen_feed = 0
                for f in friends:
                    c = crab.user_post[f].find(R.type.in_([1,2,3,5,7]) & ( R.created_on >= start_timestamp) & (R.created_on < end_timestamp)).count()
                    seen_feed += c
                csw.write_onerow([month,m,is_read_activtity,is_write_activity,len(friends),seen_feed])
    csw.end_write()
        
export_data()