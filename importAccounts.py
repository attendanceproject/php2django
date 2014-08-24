
import re
import sys

from datetime import datetime

from php2django import php2django

from accounts.models import User, Trainee, TrainingAssistant

from django.db.models import Q

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
    """
0    ID    int(11)
1    username    varchar(32)
2    encryptedPassword    varchar(32)
3    firstName    varchar(32)
4    nickName    varchar(32)
5    lastName    varchar(32)
6    middleName    varchar(32)
7    maidenName    varchar(32)
8    birthDate    date
9    gender    enum('M', 'F')
10    home_localityID    int(11)
11    address    varchar(255)
12    city    varchar(255)
13    state    varchar(255)
14    zip    varchar(10)
15    country    varchar(255)
16    active    tinyint(1)
17    maritalStatus    enum('S', 'M')
18    homePhone    varchar(14)
19    cellPhone    varchar(14)
20    workPhone    text
21    email    varchar(255)
22    lastLogin    datetime
23    lastIP    varchar(15)
    """
    # Optional: the index of the primary key
    key=0
    
    # Optional function: Return True for rows to import.
    #     Return False if the row should be skipped.
    def row_filter(self,row,importers):
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
        def gender(self,row,importers):
            if row[9]=='M':
                return 'B' 
            if row[9]=='F':
                return 'S'
            raise ValueError('gender: %s' % (row[9]))
        #is_active=17
        def is_active(self,row,importers):
            if not row[16] is None:
                return row[16]
            return False
        def phone(self,row,importers):
            cellPhone=row[19]
            if not cellPhone is None and cellPhone!='':
                return cellPhone
            homePhone=row[18]
            if not homePhone is None and homePhone!='':
                return homePhone
            workPhone=row[20]
            if not workPhone is None:
                return workPhone
            return ''
        def email(self,row,importers):
            if not row[21] is None and validateEmail(row[21]):
                try: #verify that this email isn't already used
                    if row[0] in importers['accounts.models.User'].key_map:
                        User.objects.get(Q(email=row[21]) & ~Q(pk=importers['accounts.models.User'].key_map[row[0]]))
                    else:
                        User.objects.get(Q(email=row[21]))
                except User.DoesNotExist:
                    return row[21]
            # username if email is none otherwise email
            if not row[1] is None:
                email = '%s@noemail.com' % (row[1])
                if validateEmail(email):
                    return email
            return '%s@noemail.com' % (row[0])
        def last_login(self,row,importers):
            # minimum date value if lastlogin is none
            return datetime.min if row[22] is None else row[22]

# Depends on User and TA, TODO the rest of dependencies. It is dependent on it
# self which means it will require a two-pass import
# TODO
class ImportTrainingAssistant(php2django.ImportTemplate):
    model=TrainingAssistant
    query='SELECT * FROM trainingAssistant'
    """
0    ID    int(11)
1    userID    int(11)
2    lastName    varchar(32)
3    firstName    varchar(32)
4    middleName    varchar(32)
5    birthDate    datetime
6    active    tinyint(1)
7    maritalStatus    enum('S', 'M')
8    residence    int(11)
9    outOfTown    tinyint(1)
10   approvingTAID    int(11)
    """

    key=0
        
    # Optional function: Return True for rows to import.
    #     Return False if the row should be skipped.
    def row_filter(self,row,importers):
#        if row[39] and int(row[39])==3: # remove short term part time
#            return False
        return row[1] is not None
    
    class mapping:
        account = 1 # user_id
        active = 6

# Depends on User and TA, TODO the rest of dependencies. It is dependent on it
# self which means it will require a two-pass import
# TODO
class ImportTrainee(php2django.ImportTemplate):
    model=Trainee
    #accountTypes=23 is Self Attendance
    #residenceID=100 is Commuter
    query='SELECT t.*, uat.accountTypeID IS NOT NULL as self_attendance, u.maritalStatus FROM trainee t LEFT JOIN userAccountType uat ON uat.userID=t.userID AND uat.accountTypeID=23 JOIN user u ON t.userID=u.ID and u.country<>"New Jerusalem" GROUP BY t.ID'
    """
0  ID    int(10)
1  userID    int(11)
2  dateBegin    date
3  dateEnd    date
4  firstTerm_termID    int(11)
5  secondTerm_termID    int(11)
6  thirdTerm_termID    int(11)
7  fourthTerm_termID    int(11)
8  termsCompleted    smallint(6)
9  active    tinyint(1)
10 couple    tinyint(1)
11 emergencyContact    varchar(32)
12 emergencyAddress    text
13 emergencyPhoneNumber    varchar(32)
14 emergencyPhoneNumber2    varchar(14)
15 readOldTestament    tinyint(1)
16 readNewTestament    tinyint(1)
17 trainingAssistantID    int(11)
18 mentor_userID    int(11)
19 mentor    varchar(50)
20 college    varchar(255)
21 major    varchar(255)
22 degree    text
23 gospelPreference1    varchar(255)
24 gospelPreference2    varchar(255)
25 vehicleInfoOld    varchar(50)
26 vehicleMakeOld    varchar(255)
27 vehicleModelOld    varchar(255)
28 vehicleYearOld    int(11)
29 vehicleYesNo    tinyint(1)
30 vehicleModel    varchar(50)
31 vehicleLicense    varchar(50)
32 vehicleColor    varchar(50)
33 vehicleCapacity    double(15,5)
34 teamID    int(11)
35 residenceID    int(11)
36 greekcharacter    enum('1', '2', 'c')
37 svServicesLeft    int(10)
38 officeID    int(11)
39 traineeStatusID    int(11)
40 bunkID    int(11)
41 MRType    int(11)

42 self_attendance    uat.accountTypeID IS NOT NULL
43 u.maritalStatus    enum('S','M') 
    """
    key=0
    
    class mapping:
        account = 1 # user_id
        active = 9 # is_active
        #date_created
        #type = -1 #('R', 'Regular (full-time)'),('S', 'Short-term (long-term)'),
                #('C', 'Commuter')
        def type(self,row,importers):
            if row[35] and int(row[35])==100: #residenceID=commuter
                return 'C'
            if row[39] and int(row[39])==1: #traineeStatisID=Full Time
                return 'R'
            return 'S'
        #term = -1 #models.ManyToManyField(Term, null=True)
        def term(self,row,importers):
            if 'terms.models.Term' in importers:
                old_pks=[]
                for i in [4,5,6,7]:
                    if row[i]: old_pks.append(row[i])
                return php2django.import_m2m(importer=importers['terms.models.Term'],old_pks=old_pks)
        #date_begin = 2
        def date_begin(self,row,importers):
            if row[3]: return row[3]
            #TODO consider using a heuristic to replace this with the first day of the first term attended
            return datetime.min
        date_end = 3

        TA = 17 #models.ForeignKey(TrainingAssistant, null=True, blank=True)
        #requires second pass
        #mentor = -1 #models.ForeignKey('self', related_name='mentee', null=True,
        def mentor(self,row,importers):
            if row[18]:
                mentor_user_pk = php2django.lookup_pk(User,row[18],importers)
                if mentor_user_pk:
                    try:
                        ret_val = Trainee.objects.get(account__pk=mentor_user_pk)
                        return ret_val
                    except Trainee.DoesNotExist, User.DoesNotExist:
                        sys.stderr.write('WARNING: Unable to find mentor (User_pk=%s)\n' % (mentor_user_pk))
                else:
                    sys.stderr.write('WARNING: Unable to find mentor (userID=%s)\n' % (row[18]))
            if row[19] and row[19].find(', ')!=-1:
                last_name, first_name = row[19].split(', ',1)
                try:
                    ret_val = Trainee.objects.get(account__firstname=first_name,account__lastname=last_name)
                    return ret_val
                except Trainee.DoesNotExist, User.DoesNotExist:
                    sys.stderr.write('WARNING: Unable to find mentor (%s)\n' % (row[19]))
                except Trainee.MultipleObjectsReturned:
                    sys.stderr.write('WARNING: Ambiguous mentor (%s)\n' % (row[19]))
            return None

        #locality = models.ManyToManyField(Locality)
        team = 34 #models.ForeignKey(Team, null=True, blank=True)
        
        #TODO if residenceID is 100 then leave these blank
        house = 35 #models.ForeignKey(House, null=True, blank=True)
        bunk = 40 #models.ForeignKey(Bunk, null=True, blank=True)

        # personal information
        #married = -1 #models.BooleanField(default=False)
        def married(self,row,importers):
            if row[43] and row[43]=='M':
                return True
            return False
        #spouse = -1 #models.OneToOneField('self', null=True, blank=True)
        #TODO once residences are imported check the couple field and if it is set look for another couple trainee in the same residence with the same lastname
        
        # refers to the user's home address, not their training residence
        # address = -1 #models.ForeignKey(Address, null=True, blank=True, verbose_name='home address')

        # flag for trainees taking their own attendance
        # this will be false for 1st years and true for 2nd with some exceptions.
        #self_attendance = -1 #models.BooleanField(default=False)
        def self_attendance(self,row,importers):
            if row[42]: return True
            return True if row[8]>=2 else False
            raise Exception('TODO: implement this')
        
