#!/usr/bin/python

import MySQLdb
import os
import pickle
import re
import sys
import traceback
import types

# You should create a local.py file for your django settings in the djattendance
# submodule.
settings_path = "ap.settings.local"

#Add the djattendance submodule to the search path for Python modules 
sys.path.insert(0, os.path.abspath(os.path.join('djattendance_repo','ap')))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_path)
from django.conf import settings
from django.db.models.fields import related

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

from accounts.models import User, Trainee, TrainingAssistant

# from http://stackoverflow.com/a/13653312/1549171
def absModuleInstance(o):
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__
    return module + '.' + o.__class__.__name__

def absModule(o):
    module = o.__module__
    if module is None or module == str.__class__.__module__:
        return o.__name__
    return module + '.' + o.__name__

ignore = re.compile('^__.*__$')
class ImportTemplate(object):
    # TODO if this is going to be used for generic migrations it should probably be converted to an abstract class
    key_map={}
    
    def row_filter(self,row):
        return True
            
    def get_pickle_file_name(self):
        return '%s.pickle' % (absModule(self.model))
    
    def save_key_map(self):
        filename = self.get_pickle_file_name() 
        with open(filename,'wb') as outfile:
            pickle.dump(self.key_map, outfile)
            
    def load_key_map(self):
        filename = self.get_pickle_file_name()
        with open(filename,'rb') as infile:
            self.key_map = pickle.load(infile)
    
    def import_row(self,row,**kargs):
        param = {}
        pk = None
        key = None
        
        print row
        
        # Get old primary key and see if it already has a corresponding new key
        if not self.key is None:
            if isinstance(self.key,types.FunctionType):
                key = self.key(row)
            else:
                key = row[self.key]
            if key in self.key_map:
                pk = self.key_map[key]
        
        # Use the mapping attributes and functions to convert the query row into
        # a model instance.
        for prop in self.mapping.__dict__:
            if not ignore.match(prop):
                var = self.mapping.__dict__[prop]
                if isinstance(var,types.FunctionType):
                    param[prop]=var(self.mapping,row)
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
        else:
            model_instance = self.model.objects.get(pk=pk)
            for prop, value in param.iteritems():
                model_instance.__dict__[prop]=value
            model_instance.save()
            
        print model_instance
        #except Exception, exp:
        #    print exp
    
    #kargs should contain key maps to foreign keys
    def doImport(self,**kargs):
        # load key map if it exists
        if os.path.isfile(self.get_pickle_file_name()):
            self.load_key_map()
        
        # Execute the mysql query and process the results
        cur.execute(self.query)
        result = cur.fetchall()
        try:
            for row in result:
                # apply the row filter to check whether or not to import the row
                if self.row_filter(row):
                    # import the row based on self.mapping
                    self.import_row(row,**kargs)
        # Exceptions are caught so the key_map can be saved
        except Exception as e:
            self.save_key_map()
            raise e
        
        self.save_key_map()
    
class ImportManager:
    import_lookup = {}
    finished_imports = set()

    def build_lookup_table(self,skip_if_pickle=False):
        for import_class in ImportTemplate.__subclasses__():
            model_name = absModule(import_class.model)
            import_instance = import_class()
            self.import_lookup[model_name] = import_instance 
            if skip_if_pickle and os.path.isfile(self.get_pickle_file_name()):
                self.load_key_map()
                self.finished_imports.add(model_name)
    
    def process_import(self,model_name):
        double_import = False
        self.finished_imports.add(model_name)
        import_instance = self.import_lookup[model_name]
        for attr in import_instance.model.__dict__.itervalues():
            if isinstance(attr,related.ReverseSingleRelatedObjectDescriptor):
                print "FK", absModule(attr.field.rel.to)
                #TODO write this
            if isinstance(attr,related.ReverseManyRelatedObjectsDescriptor):
                print "FKM2M", absModule(attr.field.rel.to)
                #TODO write this
        
    
    def process_imports(self):
        for model_name in self.import_lookup.iterkeys():
            if model_name in self.finished_imports: continue
            print model_name
            self.process_import(model_name)
        
        
        
        