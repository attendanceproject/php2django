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

def lookup_pk(module,old_key,importers):
    module_name = module if isinstance(module,str) else abs_module(module)
    try:
        return importers[module_name].key_map[old_key]
    except KeyError:
        return None

ignore = re.compile('^__.*__$')
class ImportTemplate(object):
    # TODO if this is going to be used for generic migrations it should probably be converted to an abstract class
    key_map={}
    
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
        pk = None
        key = None
        
        print row
        
        # Get old primary key and see if it already has a corresponding new key
        if not self.key is None:
            if isinstance(self.key,types.FunctionType):
                key = self.key(row,importers)
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
                    param[prop]=var(self.mapping,row,importers)
                else:
                    # if it is a foreign key use the key_map to look it up
                    if prop in self.model.__dict__ and row[var] and \
                            isinstance(self.model.__dict__[prop],
                            related.ReverseSingleRelatedObjectDescriptor):
                        f_model = self.model.__dict__[prop].field.rel.to
                        fk_model = abs_module(f_model)
                        if fk_model in importers and row[var] in importers[fk_model].key_map:
                            param[prop]=f_model.objects.get(pk=importers[fk_model].key_map[row[var]])
                        else:
                            sys.stderr.write("WARNING: missing foreign key! %s.%s -> %s (%s)\n"
                                % (abs_module(self.model),prop,fk_model,row[var]));
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
    
    #importers contains the importer classes for the current model and dependent models
    def doImport(self,importers):
        # load key map if it exists
        if os.path.isfile(self.get_pickle_file_name()):
            self.load_key_map()
        
        # Execute the mysql query and process the results
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

class ImportManyToMany(object):
    def __init__(self,from_model_importer,to_model_importer,query):
        self.from_importer = from_model_importer
        self.to_importer = to_model_importer
        self.query=query

    def doImport(self,importers):
        cur.execute(self.query)
        result = cur.fetchall()
        for row in result: pass
            #TODO
    
class ImportManager:
    import_lookup = {}
    finished_imports = set()
    
    # these are cleared by calling process_imports
    queued_imports = set()
    # dependency loop detection
    warning_list = []

    def build_lookup_table(self,class_list=[],skip_if_pickle=False):
        if class_list == []:
            class_list = ImportTemplate.__subclasses__()
        for import_class in class_list:
            model_name = abs_module(import_class.model)
            import_instance = import_class()
            self.import_lookup[model_name] = import_instance 
            if skip_if_pickle and os.path.isfile(self.get_pickle_file_name()):
                self.load_key_map()
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
                if isinstance(attr,related.ReverseSingleRelatedObjectDescriptor):
                    fk_model = abs_module(attr.field.rel.to)
                    print model_name, "FK", fk_model
                    if fk_model not in self.finished_imports:
                        if fk_model in self.queued_imports:
                            if fk_model == model_name:
                                double_import=True
                            else: #handle loops
                                self.warning_list.append(model_name)
                        elif fk_model in self.import_lookup:
                            # had to move this outside of loop because of:
                            # RuntimeError: dictionary changed size during iteration
                            dependencies.append(fk_model)
                        else:
                            sys.stderr.write('WARNING: Unimplemented import template for: %s (ref:%s)\n' % (fk_model,model_name))
                            continue
                    importers[fk_model]=self.import_lookup[fk_model]
                if isinstance(attr,related.ReverseManyRelatedObjectsDescriptor):
                    print model_name, "FKM2M", abs_module(attr.field.rel.to)
                    #TODO write this
            for fk_model in dependencies:
                self.process_import(fk_model,mock=mock)
                 
            if mock==False: import_instance.doImport(importers)
            self.queued_imports.remove(model_name)
            self.finished_imports.add(model_name)
            
            #handle links to self
            if double_import:
                sys.stderr.write('NOTICE: Starting repeat import for %s to catch links to self\n' % (model_name))
                if mock==False: import_instance.doImport(importers)
        except KeyError as ke:
            traceback.print_exc()
            raise Exception("Unimplemented import template for: %s" % (model_name))
        
    def process_imports(self,import_list=[],mock=False):
        self.queued_imports = set()
        self.warning_list = []
        
        if import_list==[]:
            import_list=self.import_lookup.iterkeys()
        for model_name in import_list:
            if model_name in self.finished_imports: continue
            self.process_import(model_name,mock=mock)
        
        #clean up the mess caused by foreign key reference loops
        for model_name in self.warning_list:
            self.process_import(model_name,mock=mock)
        
        self.warning_list = []
        
        
        
        