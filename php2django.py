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

from accounts.models import User
from aputils.models import State, City, Address

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
                 
            if mock==False: import_instance.doImport(importers)
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
        
        
class LoadDataManager:
    def __init__(self):
        self.queued_data = set()

    def process_load(self,model_name,params):
        if model_name == 'city':
            loadCity(params)

    def loadCity(params):    
        model_instance = City.objects.create(**param)
        model_instance.save()