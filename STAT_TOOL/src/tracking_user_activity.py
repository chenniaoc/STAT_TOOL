# -*- coding:utf-8 -*-

import config
config.DB_CONNECT_STRING = 'mysql://jiepang:j1epan9_w0za1@192.168.10.10:3300/wozai_lbs?charset=utf8'
from public import *
from tools.stat.utils import get_id
START = datetime.datetime(2012,11,30,16)
END = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1), datetime.time(16))
END = START + datetime.timedelta(hours = 48)
#START = END - datetime.timedelta(days=1)
def get_users(ds, de, ugs):
    #ds, de is utc
    ds -= datetime.timedelta(seconds=1)
    dates = []
    users = []
    while 1:
        print ds
        if User.query.filter((User.created_on > ds) & (User.created_on < de)).count() == 0:
            break
        for user in User.query.filter((User.created_on > ds) & (User.created_on < de)).order_by(User.created_on).limit(2000).all():
            if user.nick.find(u'已封禁') == -1 and not user.name.startswith('+'):
                d = (user.created_on + datetime.timedelta(hours=8)).date()
                if d not in dates:
                    dates.append(d)
                users.append([user.id, d, user.client_type])
            
            ds = user.created_on
            
    for y in dates:
        g = [u for u in users if u[1] == y]
        ugs.append(g)

def populate_users_posts(ds, de, all_posts):
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
        for r in db_slave.post.find({'_id': {'$gt': start_id, '$lt': end_id}}).sort('_id',1).limit(10000):
            if r['t'] in [1,2,3,5,7]:
                temp = all_posts.get(r['u']) or []
                temp.append(r)
                all_posts[r['u']] = temp
                
            start_id = r['_id']
                
    # return all_posts

def populate_DAU(ds, de, logs):
    #ds, de is utc
    ds = ds - datetime.timedelta(days=7)
    while ds < de:
        date = ds + datetime.timedelta(hours=8)
        with open('/home/jiepang/.logger/aru/activate_user-%s.csv' % str(date.date())) as f:
            logs[date.date()] = set([int(id) for id in f])
        ds += datetime.timedelta(days=1)
        
    # return logs

def get_friends_and_counts(user_id, date, days=7):
    #date is bj
    c_active = 0
    c_total = []
    for r in crab.user_friend[user_id].find(R.created_on < calendar.timegm((date - datetime.timedelta(hours=8) + datetime.timedelta(days=1)).timetuple())):
        c_total.append(r['user_id'])
        for i in range(0,days):
            td =  date - datetime.timedelta(days=i)
            if r['user_id'] in all_DAU.get(td, set()):
                c_active += 1
                break
                
    return c_active, len(c_total) - c_active, c_total

def get_user_data(user_id, date):
    #date is bj
    #Users	# Check-in	# Friends	# Active Friends	# KOL	# Posts in Feed	Sync SNS
    num_checkins = 0
    num_sync_sns = 0    
    for post in all_posts.get(user_id,[]):
        if (post['c'] + datetime.timedelta(hours=8)).date() == date and post['t'] == 1:
            num_checkins += 1
            num_sync_sns += post.get('ns', 0)
            
    a, ia, friend_ids = get_friends_and_counts(user_id, date)
    num_friends = len(friend_ids)
    num_active_friends = a
    num_post_seen = 0
    
    for f in friend_ids:
        # if all_posts.get(f) == None:
            # populate_users_posts([f], START, END, all_posts)
        for post in all_posts.get(f, []):
            if (post['c'] + datetime.timedelta(hours=8)).date() == date and post['t'] in [1,2,3,5,7]: 
                num_post_seen += 1
                
    return num_checkins, num_friends, num_active_friends, 0, num_post_seen, num_sync_sns

def filter_android(users):
    users_android = []
    for user in users:
        if user[2].startswith('Android'):
            users_android.append(user)
    return users_android

def filter_iphone(users):
    users_iphone = []
    for user in users:
        if user[2].find('100423') != -1 or user[2].lower().find('iphone') != -1:
            users_iphone.append(user)
    return users_iphone

def seperate_active_inactive(users, date):
    #date is bj
    active_users = []
    inactive_users = []
    for user in users:
        if user[0] in all_DAU.get(date, set()):
            active_users.append(user)
        else:
            inactive_users.append(user)
            
    return active_users, inactive_users

def get_users_by_created_on_date(users, date):
    #date is bj
    temp_users = []
    started = False
    for user in users:
        if users[1] == date:
            started = True
            temp_users.append(user)
        elif started == True:
            break
            
    return temp_users

def get_daily_data(users, date):
    num_checkins = num_friends = num_active_friends = num_kol = num_post_seen = num_sync_sns = 0
    for user in users:
        pnum_checkins, pnum_friends, pnum_active_friends, pnum_kol, pnum_post_seen, pnum_sync_sns = get_user_data(user[0], date)
        #print pnum_checkins, pnum_friends, pnum_active_friends, pnum_kol, pnum_post_seen, pnum_sync_sns
        num_checkins += pnum_checkins
        num_friends += pnum_friends
        num_active_friends += pnum_active_friends
        num_kol += pnum_kol
        num_post_seen += pnum_post_seen
        num_sync_sns += pnum_sync_sns
        
    return num_checkins, num_friends, num_active_friends, num_kol, num_post_seen, num_sync_sns

all_posts = {}
all_DAU = {}
users = []
get_users(START, END, users)
populate_users_posts(START, END, all_posts)
populate_DAU(START, END, all_DAU)
d = START
f = open('david_data.csv','a')
while d < END:
    date = (d + datetime.timedelta(hours=8)).date()
    # print date
    for ug in users:
        rdate = ug[0][1]
        if rdate > date:
            break
        daily_active_users, daily_non_active_users = seperate_active_inactive(ug, date)
        android_active = filter_android(daily_active_users)
        android_inactive = filter_android(daily_non_active_users)
        iphone_active = filter_iphone(daily_active_users)
        iphone_inactive = filter_iphone(daily_non_active_users)
        rdate, date, len(ug), get_daily_data(daily_active_users, date)
        f.write(','.join([str(rdate), str(date), 'android', 'active', str(len(android_active)), str(get_daily_data(android_active, date))]) + '\n')
        f.flush()
        f.write(','.join([str(rdate), str(date), 'android', 'inactive', str(len(android_inactive)), str(get_daily_data(android_inactive, date))]) + '\n')
        f.flush()
        f.write(','.join([str(rdate), str(date), 'ios', 'active', str(len(iphone_active)), str(get_daily_data(iphone_active, date))]) + '\n')
        f.flush()
        f.write(','.join([str(rdate), str(date), 'ios', 'inactive', str(len(iphone_inactive)), str(get_daily_data(iphone_inactive, date))]) + '\n')
        f.flush()
    d += datetime.timedelta(days=1)

f.close()
