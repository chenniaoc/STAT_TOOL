# -*- coding: utf-8 -*-


import config
config.DB_CONNECT_STRING = 'mysql://root:w0za1_j1epan9@192.168.10.5:3300/wozai_lbs?charset=utf8'

from public import *

crab = pycrab.Connection(host='10.5.3.19')
import datetime 
from common import stat_util
from common.csv_writer import CommonCsvWriter

from tools.stat.utils import get_id
INPUT_FILE_PATH = './input/stat_subway_users.csv'

def get_target_guids():
    #guids = []
    return stat_util.get_vertical_list_from_csv(INPUT_FILE_PATH, 2)

def retrive_guids(before = datetime.datetime.utcnow() - datetime.timedelta(days = 100)):
    before = datetime.datetime.utcnow() - datetime.timedelta(days = 100)
    before = datetime.datetime(*before.timetuple()[:3])
    #end = get_id('locations', 'created_on', before)
    guids = []
    #pre_sql = "select l.guid FROM locations l limit %s,%s;"
    #param = ('',str(1000))
    #print end
    #sql = 'select guid from locations l where l.guid <  '
    offset = 0
    limit = 50000
    flag = True
    retrieve_count = 0
    while retrieve_count < 2350128:
        
        for guid,c in session.query(Location.guid,Location.created_on).offset(offset).limit(limit):
            #print c
            if c <= before:
                guids.append(guid)
            retrieve_count += 1
        print len(guids)
        offset += limit
    
    return guids
    
def stat_guid_lists(target_guids):
    #start_date = datetime.datetime.utcnow() - datetime.timedelta(days = 100)
    #stat_timestamp = stat_util.convert_datetime_to_timestamp(start_date)
    #user_ids = set()
    guid_list = []
    csw = CommonCsvWriter(filename='./output/stat_checkin_lte5')
    csw.write_header(['guid','checkin_user_no'])
    
    for guid in target_guids:
        int_guid = guid_to_int(guid)
        count =  crab.location_post[int_guid].find().group(R.user_id).count()
        if count < 5:
            guid_list.append((guid,count))
            csw.write_onerow((guid,count))
        print guid
    csw.end_write()
    return guid_list

if __name__ == '__main__':
    target_guids = retrive_guids()
    
    result_guids = stat_guid_lists(target_guids)
    """
    csw = CommonCsvWriter(filename='./output/stat_checkin_lte5')
    csw.write_header(['guid','checkin_user_no'])
    csv_body = []
    for r in result_guids:
        guid = r[0]
        user_no = r[1]
        csv_body.append([guid,user_no])
    csw.write_body(csv_body) 
    csw.end_write()
    """
    