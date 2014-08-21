
import os
import re
import sys

from datetime import datetime

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

class ImportUser(php2django.ImportTemplate):
    # Required: the django model to import to
    model=User
    # Required: the mysql query for retrieving the rows to map to model instances
    query='SELECT * FROM user'
    # Optional: the index of the primary key
    key=0
    
    # Optional function: Return True for rows to import.
    #     Return False if the row should be skipped.
    def row_filter(self,row):
        if row[15]=='New Jerusalem': # remove short termers
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
        def gender(self,row,**kargs):
            if row[9]=='M':
                return 'B' 
            if row[9]=='F':
                return 'S'
            raise ValueError('gender: %s' % (row[9]))
        #is_active=17
        def is_active(self,row,**kargs):
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
        def email(self,row,**kargs):
            if not row[22] is None and validateEmail(row[22]):
                return row[22]
            # username if email is none otherwise email
            if not row[1] is None:
                email = '%s@noemail.com' % (row[1])
                if validateEmail(email):
                    return email
            return '%s@noemail.com' % (row[0])
        def last_login(self,row,**kargs):
            # minimum date value if lastlogin is none
            return datetime.min if row[22] is None else row[22]

# Depends on User and TA, TODO the rest of dependencies. It is dependent on it
# self which means it will require a two-pass import
# TODO
class ImportTrainingAssistant(php2django.ImportTemplate):
    model=TrainingAssistant
    query='SELECT * FROM trainingAssistant'
    # ID, userID, lastName, firstName, middleName, birthDate, active,
    # maritalStatus, residence, outOfTown, approvingTAID
    key=0

# Depends on User and TA, TODO the rest of dependencies. It is dependent on it
# self which means it will require a two-pass import
# TODO
class ImportTrainee(php2django.ImportTemplate):
    model=Trainee
    query='SELECT * FROM trainee'
    key=0
    
    class mapping:
        account = -1 # user_id
        active = -1 # is_active
        #date_created
        type = -1 #('R', 'Regular (full-time)'),('S', 'Short-term (long-term)'),
                #('C', 'Commuter')
        #term = -1 #models.ManyToManyField(Term, null=True)
        date_begin = -1
        date_end = -1

        TA = -1 #models.ForeignKey(TrainingAssistant, null=True, blank=True)
        #requires second pass
        mentor = -1 #models.ForeignKey('self', related_name='mentee', null=True,

        #locality = models.ManyToManyField(Locality)
        #team = -1 #models.ForeignKey(Team, null=True, blank=True)
        #house = -1 #models.ForeignKey(House, null=True, blank=True)
        #bunk = -1 #models.ForeignKey(Bunk, null=True, blank=True)

        # personal information
        married = -1 #models.BooleanField(default=False)
        spouse = -1 #models.OneToOneField('self', null=True, blank=True)
        # refers to the user's home address, not their training residence
        # address = -1 #models.ForeignKey(Address, null=True, blank=True, verbose_name='home address')

        # flag for trainees taking their own attendance
        # this will be false for 1st years and true for 2nd with some exceptions.
        self_attendance = -1 #models.BooleanField(default=False)
        
