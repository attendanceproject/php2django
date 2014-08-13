#!/usr/bin/python

import os
import re
import sys

if __name__== "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))

from php2django import php2django

from accounts.models import User, Trainee, TrainingAssistant

nonNumberRegex = re.compile('[^0-9]*')

class importUser(php2django.importTemplate):
    model=User
    query='SELECT * FROM user'
    key=1
    
    class mapping:
        firstname=3
        nickname=4
        lastname=5
        def middlename(self,row):
            value=row[6]
            return '' if value is None else value
        def maidenname(self,row):
            value=row[7]
            return '' if value is None else value
        date_of_birth=8
        gender=9
        is_active=17
        def phone(self,row):
            homePhone=row[19]
            cellPhone=row[20]
            value = nonNumberRegex.sub('',homePhone if cellPhone is None else cellPhone)
            print "PHONE:", value
            return value
        email=21
        last_login=22
        
#         def functionTest(self,queryResult):
#             return str(queryResult)

if __name__== "__main__":
    temp = importUser()
    temp.doImport()