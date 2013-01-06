# coding:utf-8
'''
Created on Dec 3, 2012

@author: Eason
'''

from public import *

#内容维护 id = 
content_maintain_id = '105'
def add_content_maintain_permission():
    
    names = []
    for a in session.query(Administrator).all():
        str_permissions = a.permissions
        permissions = str_permissions.split(',')
        if '1' in permissions or '2' in permissions or '3' in permissions:
            str_permissions += (',' + content_maintain_id)
            a.permissions = str_permissions
            #session.commit()
            print a.permissions
            ids.add(a.name)
    print names
    print len(names)
    