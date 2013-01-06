# -*- coding: utf-8 -*-


import config
config.DB_CONNECT_STRING = 'mysql://jiepang:j1epan9_w0za1@192.168.10.10:3300/wozai_lbs?charset=utf8'

from public import *

import datetime 
from common import stat_util
from common.csv_writer import CommonCsvWriter

#from tools.stat.utils import get_id

def retrive_guids(before = datetime.datetime.utcnow() - datetime.timedelta(days = 100)):
    before = datetime.datetime.utcnow() - datetime.timedelta(days = 100)
    before = datetime.datetime(*before.timetuple()[:3])
    #end = get_id('locations', 'created_on', before)
    guids = set()
    #pre_sql = "select l.guid FROM locations l limit %s,%s;"
    #param = ('',str(1000))
    #print end
    #sql = 'select guid from locations l where l.guid <  '
    offset = 0
    limit = 50000
    flag = True
    retrieve_count = 0
    csw = CommonCsvWriter(filename='./output/stat_nail_beauty')
    csw.write_header([u'found from','guid','poi_name','tip_id',u'tip_content'])
    while retrieve_count < 237442:
        
        for guid,name in session.query(Location.guid,Location.name).offset(offset).limit(limit):
            if '指甲' in name or '美甲' in name or 'nail' in name or 'nail beauty' in name:
                guids.add((guid,name))
                csw.write_onerow(('poi name',guid,name))
                print 'hited by poi name',guid,name
           
            for tip in crab.location_post[guid_to_int(guid)].find(R.type==2):
                pid = tip['post_id']
                post = db_slave.post.find_one({'_id':pid})
                if post and 'b' in post:
                    body = post['b']
                    body = body.replace('\r').replace('\n')
                    if '指甲' in body or '美甲' in body or 'nail' in body or 'nail beauty' in body:
                        guids.add((guid,name))
                        csw.write_onerow(('tip',guid,name,pid,body))
                        print 'hited by tip',guid,name
            retrieve_count += 1
        print len(guids)
        offset += limit
    csw.end_write()
    return guids
    
def stat_guid_lists(target_guids):
    #start_date = datetime.datetime.utcnow() - datetime.timedelta(days = 100)
    #stat_timestamp = stat_util.convert_datetime_to_timestamp(start_date)
    #user_ids = set()
    guid_list = []
    csw = CommonCsvWriter(filename='./output/stat_nail_beauty')
    csw.write_header(['guid','checkin_user_no'])
    
    for guid in target_guids:
        csw.write_onerow(guid)
        print guid
    csw.end_write()
    return guid_list

if __name__ == '__main__':
    target_guids = retrive_guids()
    
    #result_guids = stat_guid_lists(target_guids)
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
    