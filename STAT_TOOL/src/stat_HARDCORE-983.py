# -*- coding: utf-8 -*-

import datetime 
from common import stat_util
from common.csv_writer import CommonCsvWriter

from helpers.user import *
from tools.stat import utils
import config
config.DB_CONNECT_STRING = 'mysql://jiepang:j1epan9_w0za1@192.168.10.10:3300/wozai_lbs?charset=utf8'
from public import  *
db_slave = pymongo.Connection('192.168.10.10:27017', _connect=False, slave_okay=True)['jiepang_production']
crab = pycrab.Connection(host='10.5.3.19')



def target_uids():
    return stat_util.get_vertical_list_from_csv('./input/stat_983.csv', 2)

def get_location(_guid):
    loc = session.query(Location).filter_by(guid=_guid).first()
    loc_name = loc.name if loc else ''
    print 'get_location',_guid,loc_name
    return loc_name

def get_post_count_by_guid(guid):
    end = stat_util.convert_datetime_to_timestamp(datetime.datetime(2012,11,1) - datetime.timedelta(hours = 8))
    start = stat_util.convert_datetime_to_timestamp(datetime.datetime(2012,9,30) - datetime.timedelta(hours = 8))
    
    r = crab.location_post[guid_to_int(guid)].find((R.type.in_([1,7,10])) & (R.created_on > start)& (R.created_on < end)).count()
    #r += crab.location_post[guid_to_int(guid)].find( R.type==7& R.created_on > start& R.created_on < end).count()
    #r += crab.location_post[guid_to_int(guid)].find( R.type==10& R.created_on > start& R.created_on < end).count()
    print 'get_post_count_by_guid',guid,r
    return r

def get_f_count(userid):
    #count = session.query(func.count(Friend.from_user_id)).first()[0]

    count = crab.user_friend[int(userid)].find(R.is_friend)['num_items']
    #count = session.query(func.count(Friend.to_user_id)).filter(Friend.from_user_id == userid).first()[0]

    return count
def export_csv(uids):
    csw = CommonCsvWriter(filename='./output/stat_HARDCORE-983')
    csw.write_header([u'aid','guid','guid_name','chechin No','view','click','share'])
    csv_body = []
    
    start_date = datetime.datetime(2012,9,30) - datetime.timedelta(hours = 8)
    end_date = datetime.datetime(2012,10,30) + datetime.timedelta(hours = 16)
    start = stat_util.convert_datetime_to_timestamp(start_date)
    end = stat_util.convert_datetime_to_timestamp(end_date)
    for uid in uids:
        sina = utils.get_weibo_info(uid)
        friends_count = get_f_count(uid)
        user = user_get(uid)
        user_name = user['name']
        user_point = user['points_total']
        user_posts = []
        check_in_count = 0
        for p in crab.user_post[uid].find( (R.created_on >=start) & (R.created_on < end) & (R.location_id > 0)):
            created_on = p['created_on']
            if not check_in_count:
                check_in_count = crab.user_post[uid].find( (R.created_on >=start) & (R.created_on < end) & (R.location_id > 0)).count()
            checkin_date = datetime.datetime.fromtimestamp(created_on)
            post = db_slave.post.find_one({'_id':p['post_id']})
            loc = location_get(post['l'], 'basic')
            loc_name = loc['name']
            city = city_get(p['city']) or 'N/A'
            if city:
                city= city['name'] 
            
            body = post['b']
            photo = db.photo.find_one({'p':p['post_id']})
            has_photo = 'YES' if photo else 'NO'
            photo_link = get_photo_url(photo) + '?size=500&style=1&quality=high' if photo else 'N/A'
            sina_weibo = 'YES' if sina else 'N/A'
            sina_name = sina['screen_name'] if sina else 'N/A'
            sina_url = 'http://weibo.com/u/%d' % sina['id'] if sina else 'N/A'
            csw.write_onerow([user_name,uid,user_point,checkin_date,loc_name,check_in_count,friends_count,city,body,has_photo,photo_link ,
                             sina_weibo,sina_name ,sina_url])
    
    
    keylist = aid_data.keys()
    keylist.sort()
    for key in keylist:
        aid = key
        datas = aid_data.get(key)
        for data in datas:
            d_key_list = data.keys()
            d_key_list.sort()
            for d_key in d_key_list:
                guid = d_key
                checkin_c = get_post_count_by_guid(guid)
                loc_name = get_location(guid)
                num = data.get(d_key)
                #print aid,guid,num
                csv_body.append([aid,guid,loc_name,checkin_c,num[0],num[1],num[2]])
           
    #csw.write_body(csv_body) 
    csw.end_write()



if __name__ == '__main__':
    uids = [int (uid) for uid in  target_uids()]
    #print uids
    export_csv(uids)
