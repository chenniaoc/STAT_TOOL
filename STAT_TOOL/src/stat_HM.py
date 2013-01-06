# -*- coding: utf-8 -*-

from public import *

import datetime 
from common import stat_util
from common.csv_writer import CommonCsvWriter



def target_ad_ids():
    
    return ['3809AA426F92A9ED','E7543E29D4A7E5AB','FC8B458E646F5D58','8339E730D48E8C7C','6A24D95AECCE37','AC1E390ADA62FBD1E2A7524E1D1E37EB','3019252749E871','39342B850058C0','EE0D8AED031EF2','CF1B40948726126A5237B1D83D1ECF81','F25FE8E7D38A4EB08115E4B07DC34513','7E53EAC8E09F80A1','B59BEE14F698DDE0','A2665A30957975EC','F796C3388A2B17569111AF54929403CE','F3180C72C05A5606']


def get_posts_by_guid(guid, start_time = None, end_time = None, detail=False):
    cond = [R.type.in_([1, 3, 7, 10])]
    if start_time and end_time:
        start = convert_datetime_to_timestamp(start_time)
        end = convert_datetime_to_timestamp(end_time)
        cond.append(R.created_on >= start)
        cond.append(R.created_on < end)
    posts = []
    for post in crab.location_post[guid_to_int(guid)].find(*cond):
        if detail:
            pid = post['post_id']
            p = db_slave.post.find_one({'_id':pid})
            if p :
                posts.append(p)
        else:
            posts.append(post['post_id'])
    return posts
    
def export_csv(guids):
    csw = CommonCsvWriter(filename='./output/stat_HM')
    csw.write_header(['date','aid','guid','guid_name','chechin No','view','click','share'])
    csv_body = []
    
    
    for guid in guids:
        posts = get_posts_by_guid(guid, detail=True)
        for post in posts:
            row = [post['u'],post['l'],post['c'],post['b'].replace('\r','').replace('\n','')]
            has_photo = 'Yes' if db_slave.photo.find_one({'p':post['_id']}) else 'No'
            row.append(has_photo)
            url = 'http://jiepang.com/user/story?pid=%s' % str(post['_id'])
            row.append(url)
            csv_body.append(row)
    csw.write_body(csv_body) 
    csw.end_write()
            
    


if __name__ == '__main__':
    guids = target_ad_ids()

    #print aid_data
    export_csv(guids)