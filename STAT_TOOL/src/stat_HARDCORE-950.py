# -*- coding: utf-8 -*-

from public import *

import datetime 
from common import stat_util
from common.csv_writer import CommonCsvWriter


STAT_ACTIVATE_USER_KEY = 'stat_activate_user'
STAT_ACTIVATE_USER_KEY_2 = 'stat_activate_user_2'
STAT_CHECKIN_TIMES_KEY = 'stat_checkin_times'
STAT_REGISTER_USER_KEY = 'stat_register_user'
STAT_REGISTER_SOURCE_KEY = 'stat_register_source'
STAT_REGISTER_ERROR_KEY = 'stat_register_error'
STAT_TIP_KEY = 'stat_tip_2'
STAT_TIP_DISTRITUTE_KEY = 'stat_tip_distribute_2'
STAT_VENUE_LIST_KEY = 'stat_venue_list'
STAT_CHECKIN_CLIENT_KEY = 'stat_checkin_client'
STAT_CHECKIN_CLIENT_KEY_2 = 'stat_checkin_client_2'
STAT_USER_LOCATON_KEY = 'stat_user_location'
STAT_USER_LOCATON_KEY_2 = 'stat_user_location_2'
STAT_SYNC_KEY = 'stat_sync'
STAT_LOCATION_KEY = 'stat_location'
STAT_BOARD_KEY = 'stat_board'
STAT_S_KEY = 'stat_s'
STAT_LBA = 'stat_lba_new'
STAT_LBA_CLICK = 'stat_lba_click'
STAT_USER_FROM_BADGE_KEY = 'stat_user_from_badge_%s'
STAT_BADGE_DELIVER_KEY = 'stat_badge_deliver_%s'
STAT_AI_KEY = 'stat_ai'
#add 2012/09/13 探索统计url
STAT_EXPLORE_NO_CHECKIN = 'stat_explore_no_checkin'
STAT_EXPLORE_NEW_USER = 'stat_explore_new_user'
STAT_EXPLORE_REGISTER_SOURCE = 'stat_explore_register_source'
"""
def stat_lba(self):
        ad_id = request.params.get('ad_id')
        result = {} 
        key = STAT_LBA 
        key_2 = STAT_LBA_CLICK
        now = datetime.datetime.now() - datetime.timedelta(days=1)
        share_count = 0
        for i in range(0, (self.end - self.start).days + 1):
            date = self.start + datetime.timedelta(i)
            day = date.strftime(DATE_FORMAT[1])
            record = db.reports.find_one({'key': key, 'date': day})
            #click = db.reports.find_one({'key': key_2, 'date': day})
            #share = db.reports.find_one({'key': 'stat_lba_share', 'date': day})
            #if share:
            #    share_count += json_decode(share['data']).get(ad_id, 0)
            if not record:
                if date < now:
                    c.message += u'%s, ' % day
            else:
               data = json_decode(record['data'])
               #result.append(data)
               result = merge_dict(result, data.get(ad_id, {}))
               #if click:
               #    result = merge_dict(result, json_decode(click['data']).get(ad_id, {}))
        if c.message:
            c.message += u'数据还没有生成,请联系RD!'
        c.result = []
        view = click = 0
        for guid, num in result.iteritems():
            loc = location_get(guid, 'basic')
            if not loc:
                continue
            item = {
                    'guid': guid,
                    'name': loc['name'],
                    'view': num[0],
                    'click': num[1],
                    'rate': "%.2f%%" % (num[1] * 100.0 / num[0] if num[0] else 0),
                    'share': num[2],
                    }
            view += num[0]
            click += num[1]
            share_count += num[2]
            
            c.result.append(item)
        if view:
            rate = "%.2f%%" % (click * 100.0 / view)
            c.result.append({
                'guid': u'总数',
                'name': '',
                'view': view,
                'click': click,
                'rate': rate,
                'share': share_count 
                })
        
        start = self.start.strftime(DATE_FORMAT[0])
        end = self.end.strftime(DATE_FORMAT[0])
        
        end_time = datetime.datetime.strptime(end, '%Y-%m-%d')
        #print end_time,datetime.date.today()
        if end_time.date() >= datetime.date.today():
            end_time = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1),datetime.time())
            
        start_time = datetime.datetime.strptime(start, '%Y-%m-%d')
        detail = db.candidate_task_ad.find_one({'ad_id': int(ad_id), 'start': start_time, 'end': end_time})
        return render('stat_lba.html', {'start': start,'end': end, 'ad_id': ad_id, 'detail_status': detail and detail['type_of'] or 'N', 'file_id': str(detail['_id']) if detail else ''})
"""
def get_ads_report(aids):
    aid_data = {}
    #STAT_LBA
    for r in db.reports.find({'key':'stat_lba_new','date':{'$gte':'20121001','$lte':'20121031'}}):
        data = json_decode(r['data'])
        for aid in aids:
            d = data.get(aid)
            if d:
                if aid not in aid_data:
                    aid_data[aid] = []
                aid_data[aid].append(d)
    return aid_data

def target_ad_ids():
    ressult = ['427']
    for i in range(382,422):
        ressult.append(str(i))
    for i in range(432,450):
        ressult.append(str(i))
        
    return ressult

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
def export_csv(aid_data):
    csw = CommonCsvWriter(filename='./output/stat_HARDCORE-950')
    csw.write_header(['aid','guid','guid_name','chechin No','view','click','share'])
    csv_body = []
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
           
    csw.write_body(csv_body) 
    csw.end_write()

if __name__ == '__main__':
    aids = target_ad_ids()
    print aids
    aid_data = get_ads_report(aids)
    #print aid_data
    export_csv(aid_data)