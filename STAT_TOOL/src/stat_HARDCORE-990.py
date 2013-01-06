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
    
    
    csw = CommonCsvWriter(filename='./output/stat_HARDCORE-990-1')
    csw.write_header([u'aid','guid','guid_name','chechin No','view','click','share'])
    csv_body = []
    
    
    start_time = datetime.datetime(2012,6,14) - datetime.timedelta(hours = 8)
    start = stat_util.convert_datetime_to_timestamp(start_time)
    end_time = datetime.datetime(2012,11,14) + datetime.timedelta(hours = 16)
    end = stat_util.convert_datetime_to_timestamp(end_time)
    for guid in guids:
        _checkin_count = crab.location_post[guid_to_int(guid)].find(R.type.in_([1,3,7,10]) ).count()
        _user_count = crab.location_post[guid_to_int(guid)].find(R.type.in_([1,3,7,10])  ).group(R.user_id ).count()
        _photo_count = crab.location_post[guid_to_int(guid)].find( (R.has_photo) &R.type.in_([1,3,7,10])).count()
      
        checkin_count = crab.location_post[guid_to_int(guid)].find(R.type.in_([1,3,7,10]) & (R.created_on >=start) & (R.created_on <end)).count()
        user_count = crab.location_post[guid_to_int(guid)].find(R.type.in_([1,3,7,10])  & (R.created_on >=start) & (R.created_on <end)).group(R.user_id ).count()
        photo_count = crab.location_post[guid_to_int(guid)].find(R.type.in_([1,3,7,10]) & (R.has_photo)  & (R.created_on >=start) & (R.created_on <end)).count()
        #pid = crab.location_post[int(guid,16)].find(R.type.in_([1,3,7,10]))[0]['post_id']
        location = location_get(guid)
        if location:
            #row = [guid,location['name'],checkin_count,user_count,photo_count,' ',_checkin_count,_user_count,_photo_count]
            row = [guid,location['name'],_checkin_count,_user_count,_photo_count,' ',checkin_count,user_count,photo_count]
        print row
        csw.write_onerow(row)
    csw.end_write()
    
    csw = CommonCsvWriter(filename='./output/stat_HARDCORE-990-2')
    csw.write_header([u'aid','guid','guid_name','chechin No','view','click','share'])
    csv_body = []
    
    start_date = datetime.datetime(2012,6,14)
    end_date = datetime.datetime(2012,11,14)
    for i in range(0,(end_date - start_date).days + 1 ):
        _day = start_date + datetime.timedelta(days = i)
        start_time = _day - datetime.timedelta(hours = 8)
        end_time = _day + datetime.timedelta(hours = 16)
        start = stat_util.convert_datetime_to_timestamp(start_time)
        end = stat_util.convert_datetime_to_timestamp(end_time)
        checkin_count = 0
        user_count = 0
        photo_count = 0
        for guid in guids:
                checkin_count += crab.location_post[guid_to_int(guid)].find(R.type.in_([1,3,7,10]) & (R.created_on >=start) & (R.created_on <end)).count()
                user_count += crab.location_post[guid_to_int(guid)].find(R.type.in_([1,3,7,10])  & (R.created_on >=start) & (R.created_on <end)).group(R.user_id ).count()
                photo_count += crab.location_post[guid_to_int(guid)].find(R.type.in_([1,3,7,10]) & (R.has_photo)  & (R.created_on >=start) & (R.created_on <end)).count()
        row = [_day,checkin_count,user_count,photo_count]
        print row
        csw.write_onerow(row)
    csw.end_write()

def get_cinema_guids():
    guids = set()
    for r in db_slave.locations_categories_2.find({'cat.id':'0403'}):
        guid = r['_id']
        guids.add(guid)

    return sorted(guids)



if __name__ == '__main__':
    target_guids = target_guids()
    print target_guids
    export_csv(target_guids)
