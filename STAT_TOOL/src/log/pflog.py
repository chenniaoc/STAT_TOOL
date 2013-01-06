'''
Created on Nov 15, 2012

@author: Eason
'''

import re


class ImgLog(object):
    
    REQ_ID = 0
    TYPE = 1
    SPD_TIME = 2
    LENGTH = 3
    
    def init(self):
        return None
    
    reguler_pattern_row = '(\w+)\s(\d+)\s(\d+)\s(\d+)'
    regular_row = re.compile(reguler_pattern_row)
    @classmethod
    def read_rows(cls,filepath = '../input/img_profiling.log'):
        result = []
        if not filepath:
            return result
        with open(filepath,'r') as log_file:
            for line in log_file:
                line =  line.replace('\n','').replace('\r','')
                match =  cls.regular_row.match(line)
                if match:
                    data = match.groups()
                    result.append(data)
                else :
                    print 'not found'
        return result

    @classmethod
    def compare_to_client_log(cls,img_log_data,client_log_data):
        result = {}
        p_logs = {}
        for pdata in img_log_data:
            req_id = pdata[0]
            if req_id not in p_logs:
                p_logs[req_id] = []
            p_logs[req_id].append(pdata)
        
        for cdata in client_log_data:
            #print cdata
            c_type = cdata[ClientLog.TYPE]
            c_req_id = cdata[ClientLog.REQ_ID]
            c_res_time = cdata[ClientLog.RES_TIME]
           # print type
            if c_type != u'IMG':
                continue
            pdatas = p_logs.get(c_req_id)
            if pdatas:
                result[c_req_id] = {}
                result[c_req_id]['client_log'] = cdata
                result[c_req_id]['img_log'] = pdatas
                
                #print float(pdatas[-1][cls.SPD_TIME])* 1000 , int(c_res_time)
        #print result
        return result
    
class LengthLog(object):
    REQ_TIME = 0
    REQ_ID = 1
    SPD_TIME = 2 #spend time
    LENGTH = 3 #response length
    
    def init(self):
        return None
    
    reguler_pattern_row = '(\d+-\d+-\d+\s\d+:\d+:\d+)\s(\w+)\s(\d+\.\d+)\s(\d+)'
    regular_row = re.compile(reguler_pattern_row)
    @classmethod
    def read_rows(cls,filepath = '../input/length.log'):
        result = []
        if not filepath:
            return result
        with open(filepath,'r') as log_file:
            for line in log_file:
                line =  line.replace('\n','').replace('\r','')
                match =  cls.regular_row.match(line)
                if match:
                    data = match.groups()
                    result.append(data)
                else :
                    print 'not found'
        return result
    
    @classmethod
    def append_length_to_data(cls,length_log_data,result_data):
        log_data_dict = {}
        for log_data in length_log_data:
            _req_id = log_data[cls.REQ_ID]
            log_data_dict[_req_id] = log_data
        
        for req_id in result_data:
            print req_id
            length = ''
            log_data = log_data_dict.get(req_id) or [0,0,0,0]
            #if log_data:
                #length = log_data[cls.LENGTH]
                
            result_data[req_id]['length_log'] = log_data
    
    
    @classmethod
    def compare_to_client_log(cls,profiling_log_data,client_log_data):
        result = {}
        p_logs = {}
        for pdata in profiling_log_data:
            req_id = pdata[1]
            if req_id not in p_logs:
                p_logs[req_id] = []
            p_logs[req_id].append(pdata)
        
        for cdata in client_log_data:
            #print cdata
            c_type = cdata[ClientLog.TYPE]
            c_req_id = cdata[ClientLog.REQ_ID]
            c_res_time = cdata[ClientLog.RES_TIME]
           # print type
            if c_type != u'API':
                continue
            pdatas = p_logs.get(c_req_id)
            if pdatas:
                result[c_req_id] = {}
                result[c_req_id]['client_log'] = cdata
                result[c_req_id]['profiling_log'] = pdatas
                
                #print float(pdatas[-1][cls.SPD_TIME])* 1000 , int(c_res_time)
        #print result

class ProfilingLog(object):
    
    REQ_TIME = 0
    REQ_ID = 1
    SPD_TIME = 2 #spend time
    REQ_IP = 3
    RED_UID = 4
    REQ_API = 5
    REQ_VER = 6
    
    def init(self):
        return None
    
    reguler_pattern_row = '(\d+-\d+-\d+\s\d+:\d+:\d+)\s(\w+)\s(\d+\.\d+)\s(\d+\.\d+.\d+\.\d+)\s(\d+)\s(/.*/\w+)\s(\d+)'
    regular_row = re.compile(reguler_pattern_row)
    @classmethod
    def read_rows(cls,filepath = '../input/profiling.log'):
        result = []
        if not filepath:
            return result
        with open(filepath,'r') as log_file:
            for line in log_file:
                line =  line.replace('\n','').replace('\r','')
                #print line
                match =  cls.regular_row.match(line)
                if match:
                    data = match.groups()
                    result.append(data)
                else :
                    print 'not found'
        return result
    
    @classmethod
    def compare_to_client_log(cls,profiling_log_data,client_log_data):
        result = {}
        p_logs = {}
        for pdata in profiling_log_data:
            req_id = pdata[1]
            if req_id not in p_logs:
                p_logs[req_id] = []
            p_logs[req_id].append(pdata)
        
        for cdata in client_log_data:
            #print cdata
            c_type = cdata[ClientLog.TYPE]
            c_req_id = cdata[ClientLog.REQ_ID]
            c_res_time = cdata[ClientLog.RES_TIME]
           # print type
            if c_type != u'API':
                continue
            pdatas = p_logs.get(c_req_id)
            if pdatas:
                result[c_req_id] = {}
                result[c_req_id]['client_log'] = cdata
                result[c_req_id]['profiling_log'] = pdatas
                
                #print float(pdatas[-1][cls.SPD_TIME])* 1000 , int(c_res_time)
        #print result
        return result

class ClientLog(object):
    
    TYPE = 0
    IS_WIFI = 1
    REQ_ID = 2
    RES_TIME = 3
    
    OPEN_APP_START_TIME = 0
    OPEN_APP_MNC = 1
    OPEN_APP_MCC = 2
    
    
    
    index_map = {
                 'type':0,
                 'is_wifi':1,
                 'req_id':2,
                 'res_time':3
                 }
    
    
    def init(self):
        return None
    
    reguler_pattern_row = '(?P<type>API|IMG)\s(?P<is_wifi>\d{1})\s(?P<req_id>\w*)\s(?P<res_time>\d*)'
    regular_row = re.compile(reguler_pattern_row)
    reguler_pattern_title = '(#)\s(?P<start_time>\d+)\s(?P<mnc>\d+)\s(?P<mcc>\d+)'
    regular_title = re.compile(reguler_pattern_title)
    @classmethod
    def read_rows(cls,filepath = '../input/client_log.txt'):
        result = [[],[]] #0:titles 1:rows
        if not filepath:
            return result
        with open(filepath,'r') as log_file:
            for line in log_file:
                line =  line.replace('\n','').replace('\r','')
                match1 = cls.regular_row.match(line)
                match2 = cls.regular_title.match(line)
                if match1:
                    type =  match1.group('type')
                    is_wifi =  match1.group('is_wifi')
                    req_id =  match1.group('req_id')
                    res_time =  match1.group('res_time')
                    data = match1.groups()
                    result[1].append(data)
                    #print data
                elif match2:
                    start_time =  match2.group('start_time')
                    mnc =  match2.group('mnc')
                    mcc =  match2.group('mcc')
                    data = match2.groups()[1:]
                    result[0].append(data)
                    #print data
                else :
                    print 'not find'
        return result
#test_str = 'IMG 1 9896719d6155712cf6b6b5e9 2348'
#print ClientLog.regular_row.match(test_str).group('res_time')
"""
profiling_log_data =  ProfilingLog.read_rows()
client_log_data =  ClientLog.read_rows()
#print client_log_data
result_data = ProfilingLog.compare_to_client_log(profiling_log_data, client_log_data[1])

lengthlog = LengthLog.read_rows()
LengthLog.append_length_to_data(lengthlog,result_data)
print result_data

client_log_data =  ClientLog.read_rows()
print len(client_log_data)
imglog = ImgLog.read_rows()
print len(imglog)
result = ImgLog.compare_to_client_log(imglog, client_log_data[1])
print result 
"""