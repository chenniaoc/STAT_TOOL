# -*- coding: utf-8 -*-

from public import *

import datetime 
from common import stat_util
from common.csv_writer import CommonCsvWriter


INPUT_FILE_PATH = './input/stat_subway_users.csv'

def get_target_guids():
    #guids = []
    return stat_util.get_vertical_list_from_csv(INPUT_FILE_PATH, 2)


def stat_user_lists(target_guids):
    start_date = datetime.datetime.utcnow() - datetime.timedelta(days = 60)
    stat_timestamp = stat_util.convert_datetime_to_timestamp(start_date)
    user_ids = set()
    for guid in target_guids:
        int_guid = guid_to_int(guid)
        for r in crab.location_post[int_guid].find(R.created_on>stat_timestamp).group(R.user_id):
            user_ids.add(r['user_id'])
    
    return user_ids

if __name__ == '__main__':
    target_guids = get_target_guids()
    #print target_guids
    user_ids = stat_user_lists(target_guids)
    csw = CommonCsvWriter('subway_userlist')
    csw.write_header([u'user_id'])
    csv_body = []
    for uid in user_ids:
        csv_body.append([uid])
    csw.write_body(csv_body)
    csw.end_write()