# -*- coding: utf-8 -*-

import codecs
import re
import csv
import time
try:
    from public import *
except :
    print 'public module has not found' 


def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

"""
notice:
    encoding of csv contens must be utf-8 
"""
def get_vertical_list_from_csv(file_path,col_no=0):
    result = []
    with codecs.open(file_path, 'r' ,'utf-8') as file:
        csv_reader = unicode_csv_reader(file)
        for row in csv_reader:
            #print line
            result.append(row[col_no -1 ] )
    return result
"""
notice:
    encoding of csv contens must be utf-8 
"""
def get_list_from_csv(file_path):
    result = []
    with codecs.open(file_path, 'r' ,'utf-8') as file:
        csv_reader = unicode_csv_reader(file)
        for row in csv_reader:
            #print line
            result.append(row)
    return result

def sort_dict_by_key(target):
    result =  {}
    keylist = target.keys()
    keylist.sort()
    print keylist
    for key in keylist:
        result[key] = target.get(key)

    return result
def convert_datetime_to_timestamp(date):
    return int(time.mktime(date.timetuple()))

def get_postcount_by_guid(guid):
    crab.location_post[guid_to_int(guid)].find(R.type==1)['items']