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



def target_guids():
    return stat_util.get_vertical_list_from_csv('./input/stat_990.csv', 1)

def get_location(_guid):
    loc = session.query(Location).filter_by(guid=_guid).first()
    loc_name = loc.name if loc else ''
    print 'get_location',_guid,loc_name
    return loc_name

def get_post_count_by_guid(guid):
    end = stat_util.convert_datetime_to_timestamp(datetime.datetime(2012,11,1) - datetime.timedelta(hours = 8))
    start = stat_util.convert_datetime_to_timestamp(datetime.datetime(2012,9,30) - datetime.timedelta(hours = 8))
    
    r = crab.location_post[guid_to_int(guid)].find((R.type.in_([1,3,7,10])) & (R.created_on > start)& (R.created_on < end)).count()
    #r += crab.location_post[guid_to_int(guid)].find( R.type==7& R.created_on > start& R.created_on < end).count()
    #r += crab.location_post[guid_to_int(guid)].find( R.type==10& R.created_on > start& R.created_on < end).count()
    print 'get_post_count_by_guid',guid,r
    return r

def get_f_count(userid):
    #count = session.query(func.count(Friend.from_user_id)).first()[0]

    count = crab.user_friend[int(userid)].find(R.is_friend)['num_items']
    #count = session.query(func.count(Friend.to_user_id)).filter(Friend.from_user_id == userid).first()[0]

    return count
def export_csv(guids):
    
    
    csw = CommonCsvWriter(filename='./output/stat_HARDCORE-1023_new')
    csw.write_header([u'guid','city','poi'])
    csv_body = []
    
    
    for guid in guids:
            info = guid.split(':')
            row = [i for i in info]
            csw.write_onerow(row)
    csw.end_write()

def get_target_guids():
    guids = set()
    taget_city = [u'北京',u'上海',u'广州',u'深圳',u'成都']
    csw = CommonCsvWriter(filename='./output/stat_HARDCORE-1023_new')
    csw.write_header([u'guid','city','poi','checkin times'])
    for r in db_slave.locations_categories_2.find({'cat.id':{'$in':['0403','0401','0103']}}):
        guid = r['_id']
        location = location_get(guid)
        #print location
        if location and 'city' in location and  location['city'] in taget_city:
            check_in_count = crab.location_post[guid_to_int(guid)].find().count()
            if check_in_count >= 100:
                guids.add(str(guid) + ':' + location['city']+':'+location['name'])
                csw.write_onerow((guid,location['city'],location['name'],check_in_count))
    csw.end_write()
    return sorted(guids)



if __name__ == '__main__':
    target_guids = get_target_guids()
    print target_guids
    #export_csv(target_guids)
