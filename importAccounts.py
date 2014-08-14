#!/usr/bin/python

import os
import re
import sys

from datetime import datetime

if __name__== "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))

from php2django import php2django

from accounts.models import User, Trainee, TrainingAssistant



nonNumberRegex = re.compile('[^0-9]*')

# from http://stackoverflow.com/a/3218128/1549171
def validateEmail( email ):
    from django.core.validators import validate_email
    from django.core.exceptions import ValidationError
    try:
        validate_email( email )
        return True
    except ValidationError:
        return False

class importUser(php2django.importTemplate):
    # Required: the django model to import to
    model=User
    # Required: the mysql query for retrieving the rows to map to model instances
    query='SELECT * FROM user'
    # Optional: the index of the primary key
    key=0
    
    # Optional function: Return True for rows to import.
    #     Return False if the row should be skipped.
    def rowFilter(self,row):
        if row[15]=='New Jerusalem':
            return False
        return True
    
    # Required: a nested class which has attributes or functions which
    #     correspond to the attributes of the django model.
    #     
    #     The attributes should be set to the index of the row which contains
    #     the value to use for the attribute with the same name in the django 
    #     model.
    #     
    #     The functions accept the query result row being imported so they can
    #     return the value django will use in the model instance.
    class mapping:
        firstname=3
        nickname=4
        lastname=5
        middlename=6
        maidenname=7
        date_of_birth=8
        def gender(self,row):
            if row[9]=='M':
                return 'B' 
            if row[9]=='F':
                return 'S'
            raise ValueError('gender: %s' % (row[9]))
        #is_active=17
        def is_active(self,row):
            if not row[17] is None:
                return row[17]
            return False
        def phone(self,row):
            homePhone=row[19]
            cellPhone=row[20]
            if not cellPhone is None and cellPhone!='':
                return cellPhone
            if not cellPhone is None:
                return homePhone
            return ''
        def email(self,row):
            if not row[22] is None and validateEmail(row[22]):
                return row[22]
            # username if email is none otherwise email
            if not row[1] is None:
                email = '%s@noemail.com' % (row[1])
                if validateEmail(email):
                    return email
            return '%s@noemail.com' % (row[0])
        def last_login(self,row):
            # minimum date value if lastlogin is none
            return datetime.min if row[22] is None else row[22]
        
#         def functionTest(self,queryResult):
#             return str(queryResult)

if __name__== "__main__":
    temp = importUser()
    temp.doImport()