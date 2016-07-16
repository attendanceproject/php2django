#!/usr/bin/python

import MySQLdb
import os
import pickle
import re
import sys
import traceback
import types
import json

# You should create a local.py file for your django settings in the djattendance
# submodule.
settings_path = "ap.settings.local"

#Add the djattendance submodule to the search path for Python modules 
sys.path.insert(0, os.path.abspath(os.path.join('djattendance_repo','ap')))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_path)
from django.conf import settings
from django.db.models.fields import related
from datetime import date

#local_settings.py should be of the form:
#  
# mysql_params = {
#  'host':  "", # your host, usually localhost
#  'user':  "", # your username
#  'passwd':"", # your password
#  'db':    ""} # the database name
#
from local_settings import mysql_params
db = MySQLdb.connect(**mysql_params) # name of the data base

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor() 

from accounts.models import User
from aputils.models import State, City, Address
from bible_tracker.models import BibleReading
from houses.models import House
from teams.models import Team
from terms.models import Term

# from http://stackoverflow.com/a/13653312/1549171
def abs_module_instance(o):
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__
    return module + '.' + o.__class__.__name__

def abs_module(o):
    module = o.__module__
    if module is None or module == str.__class__.__module__:
        return o.__name__
    return module + '.' + o.__name__

def truncate_str(string,length):
    if len(string)>length:
        new_string = string[:length] 
        sys.stderr.write('WARNING: string truncated "%s" -> "%s"\n' % (string, new_string))
        return new_string
    else:
        return string

def lookup_pk(module,old_key,importers):
    module_name = module if isinstance(module,str) else abs_module(module)
    try:
        return importers[module_name].key_map[old_key]
    except KeyError:
        return None

def import_m2m(importer=None,query=None,old_pks=None,new_pks=None):
    if old_pks is None: old_pks = []
    if new_pks is None: new_pks = []
    
    if query:
        cur.execute(query)
        result = cur.fetchall()
        new_pks = []
        old_pks+=[row[0] for row in result]
    
    if importer:
        for old_pk in old_pks:
            if old_pk in importer.key_map:
                new_pks.append(importer.key_map[old_pk])
            else:
                sys.stderr.write("WARNING: missing foreign key! %s (%s)\n"
                            % (abs_module(importer.model),old_pk));
    
    if len(new_pks)==0: return []
    return importer.model.objects.filter(pk__in=new_pks)
            

ignore = re.compile('^__.*__$')
class ImportTemplate(object):
    # TODO if this is going to be used for generic migrations it should probably be converted to an abstract class
    
    def __init__(self):
        self.key_map={}
    
    def row_filter(self,row,importers):
        return True
            
    def get_pickle_file_name(self):
        return '%s.pickle' % (abs_module(self.model))
    
    def save_key_map(self):
        filename = self.get_pickle_file_name() 
        with open(filename,'wb') as outfile:
            pickle.dump(self.key_map, outfile)
            
    def load_key_map(self):
        filename = self.get_pickle_file_name()
        with open(filename,'rb') as infile:
            self.key_map = pickle.load(infile)
    
    def import_row(self,row,importers):
        param = {}
        m2m_param = {}
        pk = None
        key = None
        
        print row
        
        # Get old primary key and see if it already has a corresponding new key
        if not self.key is None:
            if isinstance(self.key,types.FunctionType):
                key = self.key(row,importers)
            else:
                key = row[self.key]
            if key and key in self.key_map:
                pk = self.key_map[key]
        
        # Use the mapping attributes and functions to convert the query row into
        # a model instance.
        for prop in self.mapping.__dict__:
            if not ignore.match(prop):
                var = self.mapping.__dict__[prop]
                if isinstance(var,types.FunctionType):
                    if prop in self.model.__dict__ and \
                            (isinstance(self.model.__dict__[prop],
                            related.ReverseManyRelatedObjectsDescriptor) or\
                            isinstance(self.model.__dict__[prop],
                            related.ManyRelatedObjectsDescriptor)):
                        m2m_param[prop]=var(self.mapping,row,importers)
                    else:
                        param[prop]=var(self.mapping,row,importers)
                else:
                    # if it is a foreign key use the key_map to look it up
                    if prop in self.model.__dict__ and \
                            isinstance(self.model.__dict__[prop],
                            related.ReverseSingleRelatedObjectDescriptor):
                        if row[var]:
                            f_model = self.model.__dict__[prop].field.rel.to
                            fk_model = abs_module(f_model)
                            if fk_model in importers and row[var] in importers[fk_model].key_map:
                                param[prop]=f_model.objects.get(pk=importers[fk_model].key_map[row[var]])
                            else:
                                sys.stderr.write("WARNING: missing foreign key! %s.%s -> %s (%s)\n"
                                % (abs_module(self.model),prop,fk_model,row[var]));
                        else:
                            param[prop]=None
                    else:
                        param[prop]=row[var]
                    
        print param
        
        # If an instance doesn't already exist create a new one and update the
        # key_map which stores the primary key relationship between the old
        # models and the new models
        if pk is None:
            model_instance = self.model.objects.create(**param)
            model_instance.save()
            if not key is None:
                self.key_map[key]=model_instance.pk
            if len(m2m_param)>0:
                model_instance.__dict__.update(m2m_param)
                model_instance.save()
        else:
            model_instance = self.model.objects.get(pk=pk)
            model_instance.__dict__.update(param)
            model_instance.__dict__.update(m2m_param)
            model_instance.save()
            
        print model_instance
        #except Exception, exp:
        #    print exp
    
    #importers contains the importer classes for the current model and dependent models
    def doImport(self,importers):
        # load key map if it exists
        if os.path.isfile(self.get_pickle_file_name()):
            self.load_key_map()
        
        # Execute the mysql query and process the results
        if isinstance(self.query,types.FunctionType):
            result = self.query(cur)
        else:
            cur.execute(self.query)
            result = cur.fetchall()
        try:
            for row in result:
                # apply the row filter to check whether or not to import the row
                if self.row_filter(row,importers):
                    # import the row based on self.mapping
                    self.import_row(row,importers)
        # Exceptions are caught so the key_map can be saved
        except Exception as e:
            self.save_key_map()
            traceback.print_exc()
            raise e
        
        self.save_key_map()

    # def doImport_biblereading(self, importers):
    #     id_key_map = {}
    #     filename = 'accounts.models.User.pickle'
    #     with open(filename,'rb') as infile:
    #         id_key_map = pickle.load(infile)
        
    #     # Execute the mysql query and process the results
    #     if isinstance(self.query,types.FunctionType):
    #         result = self.query(cur)
    #     else:
    #         cur.execute(self.query)
    #         result = cur.fetchall()
    #     try:
    #         for row in result:
    #             # apply the row filter to check whether or not to import the row
    #             if self.row_filter(row,importers):
    #                 # import the row based on self.mapping
    #                 self.import_row_biblereading(row,importers)
    #     # Exceptions are caught so the key_map can be saved
    #     except Exception as e:
    #         self.save_key_map()
    #         traceback.print_exc()
    #         raise e
        
    #     self.save_key_map() 

biblebook_map = {
        1:'1_0', 2:'1_1', 3:'1_2', 4:'1_3', 5:'1_4', 6:'1_5', 7:'1_6', 8:'1_7', 9:'1_8', 10:'1_9',
        11:'1_10', 12:'1_11', 13:'1_12', 14:'1_13', 15:'1_14', 16:'1_15', 17:'1_16', 18:'1_17',
        19:'1_18', 20:'1_19', 21:'1_20', 22:'1_21', 23:'1_22', 24:'1_23', 25:'1_24', 26:'1_25',
        27:'1_26', 28:'1_27', 29:'1_28', 30:'1_29', 31:'1_30', 32:'1_31', 33:'1_32', 34:'1_33',
        35:'1_34', 36:'1_35', 37:'1_36', 38:'1_37', 39:'1_38', 40:'1_39', 41:'1_40', 42:'1_41',
        43:'1_42', 44:'1_43', 45:'1_44', 46:'1_45', 47:'1_46', 48:'1_47', 49:'1_48', 50:'1_49',
        51:'1_50', 52:'1_51', 53:'1_52', 54:'1_53', 55:'1_54', 56:'1_55', 57:'1_56', 58:'1_57',
        59:'1_58', 60:'1_59', 61:'1_60', 62:'1_61', 63:'1_62', 64:'1_63', 65:'1_64', 66:'1_65',
        78:'2_39', 79:'2_40', 80:'2_41', 81:'2_42', 82:'2_43', 83:'2_44', 84:'2_45', 85:'2_46',
        86:'2_47', 87:'2_48', 88:'2_49', 89:'2_50', 90:'2_51', 91:'2_52', 92:'2_53', 93:'2_54',
        94:'2_55', 95:'2_56', 96:'2_57', 97:'2_58', 98:'2_59', 99:'2_60', 100:'2_61', 101:'2_62',
        102:'2_63', 103:'2_64', 104:'2_65',
    }
    
class ImportManager:
    
    def __init__(self):
        self.import_lookup = {}
        self.finished_imports = set()
        
        # these are cleared by calling process_imports
        self.queued_imports = set()
        # dependency loop detection
        self.warning_list = set()

    def build_lookup_table(self,class_list=None,skip_if_pickle=False):
        if class_list is None:
            class_list = ImportTemplate.__subclasses__()
        for import_class in class_list:
            model_name = abs_module(import_class.model)
            import_instance = import_class()
            self.import_lookup[model_name] = import_instance 
            if skip_if_pickle and os.path.isfile(import_instance.get_pickle_file_name()):
                import_instance.load_key_map()
                self.finished_imports.add(model_name)
    
    def process_import(self,model_name,mock=False):
        double_import = False
        self.queued_imports.add(model_name)
        try:
            import_instance = self.import_lookup[model_name]
            importers={}
            importers[model_name]=import_instance
            dependencies=[]
            for attr in import_instance.model.__dict__.itervalues():
                fk=False
                if isinstance(attr,related.ReverseSingleRelatedObjectDescriptor):
                    fk_model = abs_module(attr.field.rel.to)
                    print model_name, "FK", fk_model
                    fk=True
                if isinstance(attr,related.ReverseManyRelatedObjectsDescriptor):
                    fk_model = abs_module(attr.field.rel.to)
                    print model_name, "FKM2M", fk_model
                    fk=True
                if fk:
                    if fk_model not in self.finished_imports:
                        if fk_model in self.queued_imports: #handle loops
                            self.warning_list.add(model_name)
                        elif fk_model in self.import_lookup:
                            # had to move this outside of loop because of:
                            # RuntimeError: dictionary changed size during iteration
                            dependencies.append(fk_model)
                        else:
                            sys.stderr.write('WARNING: Unimplemented import template for: %s (ref:%s)\n' % (fk_model,model_name))
                            continue
                    importers[fk_model]=self.import_lookup[fk_model]
            for fk_model in dependencies:
                self.process_import(fk_model,mock=mock)
                 
            if mock==False: 
                if model_name=='BibleReading':
                    import_instance.doImport_biblereading(importers)
                else:
                    import_instance.doImport(importers)
            self.queued_imports.remove(model_name)
            self.finished_imports.add(model_name)
        except KeyError as ke:
            traceback.print_exc()
            raise Exception("Unimplemented import template for: %s" % (model_name))
        
    def process_imports(self,import_list=[],mock=False):
        self.queued_imports = set()
        self.warning_list = set()
        
        if import_list==[]:
            import_list=self.import_lookup.iterkeys()
        for model_name in import_list:
            if model_name in self.finished_imports: continue
            self.process_import(model_name,mock=mock)
        
        #clean up the mess caused by foreign key reference loops
        for model_name in self.warning_list:
            sys.stderr.write('NOTICE: Starting repeat import for %s to handle dependency loop\n' % (model_name))
            if mock==False: self.process_import(model_name,mock=mock)
        
        self.warning_list = set()

    def add_user_info(self):
        id_team_map = {}
        query= 'SELECT * FROM team'
        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                if row[0] and row[1]:
                    id_team_map[row[0]]= row[1]
        except Exception as e:
            traceback.print_exc()
            raise e 

        id_house_map = {}
        query= 'SELECT * FROM residence'
        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                if row[0] and row[1]:
                    id_house_map[row[0]]= row[1]
        except Exception as e:
            traceback.print_exc()
            raise e 

        id_ta_map = {}
        query= 'SELECT * FROM trainingAssistant'
        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                if row[0] and row[2] and row[3]:
                    id_ta_map[row[0]]= (row[2], row[3])
        except Exception as e:
            traceback.print_exc()
            raise e 

        # Add additional information to User objects
        query='SELECT u.*, ut.accountTypeID IS NOT NULL as self_attendance, ta.userID, t.* FROM user u \
            LEFT JOIN userAccountType ut ON u.ID=ut.userID AND ut.accountTypeID=23 \
            LEFT JOIN trainingAssistant ta ON u.ID=ta.userID \
            LEFT JOIN trainee t ON u.ID=t.userID \
            GROUP BY u.ID'

        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                if row[15]!='New Jerusalem': # not short termer
                    user = User.objects.filter(email=row[21]).first()
                    if user:
                        if row[43] and row[43] in id_ta_map: # TA information
                            ta_ln, ta_fn = id_ta_map[row[43]]
                            ta = User.objects.filter(firstname=ta_fn,lastname=ta_ln).first()
                            if ta:
                                user.TA = ta
                                user.save()

                        if row[60] and row[60] in id_team_map: # Team information
                            team = Team.objects.filter(name=id_team_map[row[60]]).first()
                            user.team = team
                            user.save()

                        if row[61] and row[61] in id_house_map: # House information
                            house_name = id_house_map[row[61]]
                            house = House.objects.filter(name=house_name).first()
                            if house:
                                user.house = house
                                user.save()

                        # Fill in terms_attended info
                        if row[30]:
                            term = Term.objects.filter(id=row[30]).first()
                            user.terms_attended.add(term)
                        if row[31]:
                            term = Term.objects.filter(id=row[31]).first()
                            user.terms_attended.add(term)
                        if row[32]:
                            term = Term.objects.filter(id=row[32]).first()
                            user.terms_attended.add(term)
                        if row[33]:
                            term = Term.objects.filter(id=row[33]).first()
                            user.terms_attended.add(term)
                        user.save()

        except Exception as e:
            traceback.print_exc()
            raise e 


    def import_training_houses(self):
        query= 'SELECT * FROM residence'
        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                if row[2]==1 or row[2]==2 or row[2]==3:
                    address = Address.objects.filter(address1=row[4], zip_code=row[7]).first()
                    if address:
                        gender = 'B'
                        used = True
                        if row[2]==2:
                            gender = 'S'
                        if row[2]==3:
                            gender = 'C'
                        if row[11]==0:
                            used = False
                        house = House(name=row[1], address=address, gender=gender,used=used)
                        house.save()
        except Exception as e:
            traceback.print_exc()
            raise e 

    def process_biblereading_import(self):
        id_key_map = {}
        query= 'SELECT * FROM user'
        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                if row[0] and row[21]:
                    id_key_map[row[0]]= row[21]
        except Exception as e:
            traceback.print_exc()
            raise e 

        id_to_user_id = {}
        query= 'SELECT * FROM trainee'
        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                id_to_user_id[row[0]] = row[1]
        except Exception as e:
            traceback.print_exc()
            raise e

        # Execute the mysql query and process the results
        query= 'SELECT * FROM br_trainee_book'
        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                if row[2] and row[2] in id_to_user_id:
                    user_id = id_to_user_id[row[2]]
                    if user_id in id_key_map:
                        user = User.objects.filter(email=id_key_map[user_id]).first()
                        if user:
                            br = None
                            if BibleReading.objects.filter(trainee=user).exists():
                                #update
                                br = BibleReading.objects.filter(trainee=user).first()
                            else:
                                # create
                                br = BibleReading(trainee=user, weekly_reading_status={}, books_read={}) 
                                br.save()
                            if br:
                                self.update_biblebooks(br, row)
                    else:
                        print row[2]
                        print user_id
                        print ''
                else:
                    print row[2]
                    print ''
        except Exception as e:
            traceback.print_exc()
            raise e 

        # Execute the mysql query and process the results
        query= 'SELECT * FROM br_daily_log'
        if isinstance(query,types.FunctionType):
            result = query(cur)
        else:
            cur.execute(query)
            result = cur.fetchall()
        try:
            for row in result:
                # apply the row filter to check whether or not to import the row
                if not row[1]==0:
                    if row[1] and row[1] in id_to_user_id:
                        user_id = id_to_user_id[row[1]]
                        if user_id in id_key_map:
                            user = User.objects.filter(email=id_key_map[user_id]).first()
                            if user:
                                br = None
                                if BibleReading.objects.filter(trainee=user).exists():
                                    #update
                                    br = BibleReading.objects.filter(trainee=user).first()
                                else:
                                    # create
                                    br = BibleReading(trainee=user, weekly_reading_status={}, books_read={}) 
                                    br.save()
                                if not br==None:
                                    self.update_biblereading(br, row)
        except Exception as e:
            traceback.print_exc()
            raise e 

    def update_biblebooks(self, br, row):
        if row[1]:
            if row[1]==76 or row[1]==77:
                return
            else:
                book_id = row[1]
                book_code = biblebook_map[book_id]
                if book_code not in br.books_read:
                    br.books_read[book_code] = "Y"
                    br.save()          

    def update_biblereading(self, br, row):
        br_date = row[2]
        status = row[3]
        finalized = row[4]
        term_week_code, mod = self.get_term_week_code(br_date)

        if term_week_code not in br.weekly_reading_status:
            br.weekly_reading_status[term_week_code] = "{\"status\": \"_______\", \"finalized\": \"N\"}"
        json_weekly_reading = json.loads(br.weekly_reading_status[term_week_code])
        weekly_status = json_weekly_reading['status']
        updated_weekly_status = self.update_weekly_status(weekly_status, mod, status)
        json_weekly_reading['status'] = updated_weekly_status
        hstore_weekly_reading = json.dumps(json_weekly_reading)
        br.weekly_reading_status[term_week_code] = hstore_weekly_reading
        br.save()

    def get_term_week_code(self, br_date):
        code = ''
        week = ''
        mod = 0
        month = br_date.month
        year = br_date.year
        if year==2016:
            code += '20_'
            week, mod = self.find_week(20, br_date)
        elif year==2015 and month>=8:
            code += '19_'
            week, mod = self.find_week(19, br_date)
        else:
            code += '18_'
            week, mod = self.find_week(18, br_date)
        code += week
        return code, mod

    def find_week(self, term_code, br_date):
        day_delta = 0
        if term_code==18:
            # start date 2/16/2015
            start_date = date(2015,2,16)
        if term_code==19:
            # start date 8/10/2015
            start_date = date(2015,8,10)
        if term_code==20:
            # start date 2/22/2016
            start_date = date(2016,2,22)
        day_delta = (br_date-start_date).days
        week = day_delta/7
        mod = day_delta%7
        return str(week), mod

    def update_weekly_status(self, weekly_status, mod, status):
        s = list(weekly_status)
        if mod < len(s):
            s[mod] = status
            return "".join(s)
        return weekly_status

class LoadDataManager:
    def __init__(self):
        self.queued_data = set()

    def process_load(self,model_name,params):
        if model_name == 'city':
            loadCity(params)

    def loadCity(params):    
        model_instance = City.objects.create(**param)
        model_instance.save()