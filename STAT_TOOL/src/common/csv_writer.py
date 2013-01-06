# -*- coding: utf-8 -*-

import codecs
import csv

class CommonCsvWriter:
    
    def __init__(self,filename = ''):
        self.rows_count = 0
        self.csv_file = codecs.open( filename  + '.csv','w' ,'utf-8')
        self.writer = csv.writer(self.csv_file,CustomFormat())
        

    def write_header(self , header = None):
        if self.rows_count:
            raise Exception('已经有header了')
        self.writer.writerow(header)
        self.rows_count +=1
        
    def write_onerow(self,row = None):
        if self.rows_count < 1 :
            raise Exception('还没有有写header')
        self.writer.writerow(row)
        self.rows_count +=1
    
    def write_body(self,body = None):
        if self.rows_count < 1 :
            raise Exception('还没有有写header')
        self.writer.writerows(body)
        self.rows_count +=1

    def end_write(self):
        self.csv_file.flush()
        self.csv_file.close()

class CustomFormat(csv.excel):
    quoting   = csv.QUOTE_ALL
