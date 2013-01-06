# -*- coding: utf-8 -*-

import datetime 
from common import stat_util
from common.csv_writer import CommonCsvWriter
from tools.stat.utils import get_id
from helpers.user import *
from tools.stat import utils
#import config
#config.DB_CONNECT_STRING = 'mysql://jiepang:j1epan9_w0za1@192.168.10.10:3300/wozai_lbs?charset=utf8'
from public import  *
#db_slave = pymongo.Connection('192.168.10.10:27017', _connect=False, slave_okay=True)['jiepang_production']
#crab = pycrab.Connection(host='10.5.3.19')

from operator import itemgetter, attrgetter


EXCLUDE_TYPE_OF = set(['H','Z','N','F'])
def __is_virtual_loc(guid):
    # guid = location_str14(id)
    user_id_type_of = dicts.location_attr.get(guid)
    if user_id_type_of and user_id_type_of[1] in EXCLUDE_TYPE_OF:
        return True
    return False


def populate_users_posts(ds, de, all_posts ,all_users):
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
            if r.get('l') and not r.get('del'):
                guid = r.get('l')
                if __is_virtual_loc(guid):
                    continue
                #post_data = dict(r)
                del r['l']
                all_posts[r['_id']] = r
                all_users.add(r['u'])
                
            


def get_user_posts(uid,start_date,end_date,all_posts):
    crab_cond = [R.privacy != 1] #非【仅自己可见】 
    crab_cond.append(R.location_id)
    if start_date:
        start_timestamp = stat_util.convert_datetime_to_timestamp(start_date)
        crab_cond.append(R.created_on >= start_timestamp)
    if end_date:
        end_timestamp = stat_util.convert_datetime_to_timestamp(end_date)
        crab_cond.append(R.created_on <= end_timestamp)
    
    posts_by_month = [[] for r in range(0,12)]
    print crab.user_post[uid].find(*crab_cond).count()
    for crab_post in  crab.user_post[uid].find(*crab_cond):
        pid = crab_post['post_id']
        #mongo_post = db_slave.post.find_one({'_id':pid}) #"nl"= number of likes;"nc" = number of comments
        mongo_post = None
	if all_posts and all_posts.get(pid):
            mongo_post = all_posts.get(pid)

            #mongo_post = db_slave.post.find_one({'_id':pid})
        if not mongo_post:
            mongo_post = db_slave.post.find_one({'_id':pid})
        if not mongo_post:
            continue
        
        guid = mongo_post.get('l')
        if __is_virtual_loc(guid):
            continue
        has_photo = 1 if crab_post['has_photo'] == -1 else 0
        if mongo_post and not mongo_post.get('del'):
            created_on = mongo_post.get('c')
            week_idx = (created_on + datetime.timedelta(hours=8)).month - 1
            total_feedback = (mongo_post.get('nc') or 0 )+ (mongo_post.get('nl') or 0)
            mongo_post['total_feedback'] = total_feedback
            mongo_post['has_photo'] = has_photo
            posts_by_month[week_idx].append(mongo_post)
            
    # sort fetched posts
    for idx in range(0,12):
        posts_by_month[idx] = sorted(posts_by_month[idx],key=itemgetter('has_photo','total_feedback'),reverse=True) # sort in DESC
        #posts_by_month[idx] = sorted(posts_by_month[idx],key=lambda post: post.get('has_photo'))
        posts_by_month[idx] = posts_by_month[idx][:3] # truncate number of elements to 3
        posts_by_month[idx] = sorted(posts_by_month[idx],key=itemgetter('c')) # sort by created_on in ASC
        
    #print posts_by_month
    return posts_by_month

def fetch_all_loc_cate():
    guid_cate = {}
    
    for r in db_slave.locations_categories_2.find().sort('_id',1):
        if len(r['cat']) < 1:
            continue
        guid_cate[r['_id']] = r['cat'][0]
        
    return guid_cate


def create_cate_data_monthly():
    
    start_month = 12
    end_month = 12
    guid_cate = fetch_all_loc_cate()
    user_cate = {}
    exists_count = 0
    cursor = db_slave.stat_journal_2012.find({},{'_id':1,'mc':1},timeout = False)
    try:
        for r in cursor:
            if 'mc' not in r:
                continue
	    old_mc = r['mc']
            exists_count += 1 
            uid = r['_id']
            user_cate[uid] = [[]for r in range(0,12)]
            for month in range(start_month,end_month + 1):
                start_date = datetime.datetime(2012,month,1) -  datetime.timedelta(hours = 8)
                end_date = start_date + datetime.timedelta(days = 31)
                if end_date.month != month:
                    end_date = start_date + datetime.timedelta(days = 30)
                if end_date.month != month:
                    end_date = start_date + datetime.timedelta(days = 29)
                
                start_timestamp = stat_util.convert_datetime_to_timestamp(start_date)
                end_timestamp = stat_util.convert_datetime_to_timestamp(end_date)
                cate_count_dict = {}
                for p in crab.user_post[uid].find( (R.privacy != 1) & R.location_id & (R.created_on >= start_timestamp) & (R.created_on <= end_timestamp)).group(R.location_id):
                    loc_id = p['location_id']
                    #pid = p['post_id']
                    #mongo_post = db_slave.post.find_one({'_id':pid}) or {}
                    #guid = mongo_post.get('l')
                    guid ='%x' % loc_id 
                    if not guid:
                        continue
                    short_guid = guid[:14]
                    short_guid = short_guid.upper()
                    #print short_guid
                    #categories = db_slave.locations_categories_2.find_one({'_id':short_guid}) or {}
                    #cate = categories.get('cat')[0] if 'cat' in categories and len(categories['cat']) > 0 else ''
                    cate = guid_cate.get(short_guid)
                    if not cate:
                        continue
                    cate_id = cate.get('id') or None
                    if not cate_id:
                        continue
                    
                    if cate_id not in cate_count_dict:
                        cate_count_dict[cate_id] = 0
                    cate_count_dict[cate_id] = cate_count_dict[cate_id] + 1
                frequently_cate = []
                if cate_count_dict:
                #find 3 gone frequently categories
                    while True:
                        if len(frequently_cate) > 2 or len(frequently_cate) > len(cate_count_dict):
                            break
                        max_cate ,max_count = max(cate_count_dict.iteritems(),key = lambda cate:cate[1])
                        del cate_count_dict[max_cate]
                        #frequently_cate[max_cate] = max_count
                        frequently_cate.append({'cid':max_cate,'c':max_count})
                #print cate_count_dict
                
                user_cate[uid][month - 1] = frequently_cate
                    
                #max_locations = []
                #while True:
                #    if len(max_locations) >= 3
            
            print uid
	    if user_cate[uid][11]:
		
		if type(old_mc) != list:
		    db.stat_journal_2012.update({'_id': uid}, {'$set': {'mc':user_cate[uid]}})
		else:
            	    db.stat_journal_2012.update({'_id': uid}, {'$set': {'mc.11':user_cate[uid][11]}})
    finally:
        print 'newly appended user count is ',exists_count
        cursor.close()

def create_data_monthly():
    user_posts = {}
    for month in range(12,13):
        all_posts = {}
        all_users = set()
        start_date = datetime.datetime(2012,month,1) -  datetime.timedelta(hours = 8)
        end_date = start_date + datetime.timedelta(days = 31)
        if end_date.month != month:
            end_date = start_date + datetime.timedelta(days = 30)
        if end_date.month != month:
            end_date = start_date + datetime.timedelta(days = 29)
        print month , start_date , end_date
        populate_users_posts(start_date,end_date,all_posts,all_users)
        
        for uid in all_users:
            if uid not in user_posts:
                user_posts[uid] = [[] for r in range(0,12)]
            
            result = get_user_posts(uid,start_date,end_date,all_posts)
            month_idx = month - 1
            user_posts[uid][month_idx] = result[month_idx]
            
            
            """
            exist_user_data = db_slave.stat_journal_2012.find_one({'_id': uid})
            if exist_user_data:
                data = exist_user_data
            else:
                data = {}
                data['md'] = [[] for r in range(0,12)] #month data
            #for idx,r in enumerate(result) :
                #post = {'p':r['_id']}
            #month_idx = month - 1
            r = result[month_idx]
            data['md'][month_idx] = [{'pid':p['_id'] , 'nc':p.get('nc') or 0  , 'nl':p.get('nl') or 0, 'hp':p['has_photo'],'c':p['c']  } for p in r]
            
            db.stat_journal_2012.update({'_id': uid}, {'$set': data},upsert=True)
            """
    #print user_posts
        
    for uid in user_posts:
        months_data = user_posts.get(uid)
        exist_user_data = db_slave.stat_journal_2012.find_one({'_id': uid})
        if exist_user_data:
            data = exist_user_data
        else:
            data = {}
            data['md'] = [[] for r in range(0,12)] #month data
        for month_idx in range(0,12):
            r = months_data[month_idx]
            if r:
                data['md'][month_idx] = [{'pid':p['_id'] , 'nc':p.get('nc') or 0  , 'nl':p.get('nl') or 0, 'hp':p['has_photo'],'c':p['c']  } for p in r]
        new_data = dict(data)
	if '_id' in new_data:
		del new_data['_id']
	db.stat_journal_2012.update({'_id': uid}, {'$set': new_data},upsert=True)
     

def create_data():
    global all_users
    
    """
    for start_month in range(1,13):
        start_date = datetime.datetime(2012,start_month,1) - datetime.timedelta(hours = 8)
        end_date = start_date + datetime.timedelta(days = 30 ,hours = 24 )
    """
    
    start_date = datetime.datetime(2012,1,1) - datetime.timedelta(hours = 8)
    end_date = datetime.datetime(2012,12,31) +  datetime.timedelta(hours = 8)
    populate_users_posts(start_date,end_date,all_posts,all_users)
    #all_users = list(all_users)[:100]
    
    for uid in all_users:
        result = get_user_posts(uid,start_date,end_date)
        data = {}
        data['md'] = [[] for r in range(0,12)] #month data
        for idx,r in enumerate(result) :
            #post = {'p':r['_id']}
            data['md'][idx] = [{'pid':p['_id'] , 'nc':p.get('nc') or 0  , 'nl':p.get('nl') or 0, 'hp':p['has_photo'],'c':p['c']  } for p in r]
        
        db.stat_journal_2012.update({'_id': uid}, {'$set': data},upsert=True)
    
    """
    result = get_user_posts(uid,start_date,end_date)
    data = {}
    data['md'] = [[] for r in range(0,12)] #month data
    for idx,r in enumerate(result) :
        month = idx + 1
        print 'title:' , month , '月的签到信息有',len(r),'条' 
        data['md'][idx] = [{'pid':p['_id'] , 'nc':p.get('nc') or 0  , 'nl':p.get('nl') or 0, 'hp':p['has_photo'],'c':p['c']  } for p in r]
        for post in r:
            print ('       pid:%d : , has_photo:%s , created_on:%s , number of likes:%s , number of comments:%s ,total_feedback:%s') % \
                                (post['_id'],post['has_photo'],post['c'],str(post.get('nl') or 0 ),str(post.get('nc') or 0),str(post.get('total_feedback')))
    
    db.stat_journal_2012.update({'_id': uid}, {'$set': data},upsert=True)
    """

#test case for one user
def test_by_one_uid(uid):
    start_date = datetime.datetime(2012,12,1) - datetime.timedelta(hours = 8)
    end_date = datetime.datetime(2012,12,31) +  datetime.timedelta(hours = 8)
    #populate_users_posts(start_date,end_date,all_posts,all_users)
    """
    for uid in all_users:
        result = get_user_posts(uid,start_date,end_date)
        data = {}
        data['md'] = [[] for r in range(0,12)] #month data
        for idx,r in enumerate(result) :
            #post = {'p':r['_id']}
            data['md'][idx].append(r['_id'])
        
        db.stat_journal_2012.update({'_id': uid}, {'$set': data})
    """
    result = get_user_posts(uid,start_date,end_date)
    data = {}
    data['md'] = [[] for r in range(0,12)] #month data
    for idx,r in enumerate(result) :
        month = idx + 1
        print 'title:' , month , '月的签到信息有',len(r),'条' 
        data['md'][idx] = [{'pid':p['_id'] , 'nc':p.get('nc') or 0  , 'nl':p.get('nl') or 0, 'hp':p['has_photo'],'c':p['c']  } for p in r]
        for post in r:
            print ('       pid:%d : , has_photo:%s , created_on:%s , number of likes:%s , number of comments:%s ,total_feedback:%s') % \
                                (post['_id'],post['has_photo'],post['c'],str(post.get('nl') or 0 ),str(post.get('nc') or 0),str(post.get('total_feedback')))
    
    db.stat_journal_2012.update({'_id': uid}, {'$set': data},upsert=True)

if __name__ == '__main__':
    #test_by_one_uid(923529358)
    #create_data_monthly()
    create_cate_data_monthly()
    #export_csv(target_guids)
