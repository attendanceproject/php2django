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
settingsPath = "ap.settings.local"

#Add the djattendance submodule to the search path for Python modules 
sys.path.insert(0, os.path.abspath(os.path.join('djattendance_repo','ap')))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", settingsPath)
from django.conf import settings

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
class importTemplate:
    keyMap={}
    
    def rowFilter(self,row):
        return True
            
    def getPickleFileName(self):
        return '%s.pickle' % (absModule(self.model))
    
    def saveKeyMap(self):
        filename = self.getPickleFileName() 
        with open(filename,'wb') as outfile:
            pickle.dump(self.keyMap, outfile)
            
    def loadKeyMap(self):
        filename = self.getPickleFileName()
        with open(filename,'rb') as infile:
            self.keyMap = pickle.load(infile)
    
    def importRow(self,row):
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
            if key in self.keyMap:
                pk = self.keyMap[key]
        
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
        # keyMap which stores the primary key relationship between the old
        # models and the new models
        if pk is None:
            modelInstance = self.model.objects.create(**param)
            modelInstance.save()
            if not key is None:
                self.keyMap[key]=modelInstance.pk
        else:
            modelInstance = self.model.objects.get(pk=pk)
            for prop, value in param.iteritems():
                modelInstance.__dict__[prop]=value
            modelInstance.save()
            
        print modelInstance
        #except Exception, exp:
        #    print exp
    
    def doImport(self):
        # load key map if it exists
        if os.path.isfile(self.getPickleFileName()):
            self.loadKeyMap()
        
        # Execute the mysql query and process the results
        cur.execute(self.query)
        result = cur.fetchall()
        try:
            for row in result:
                # apply the row filter to check whether or not to import the row
                if self.rowFilter(row):
                    # import the row based on self.mapping
                    self.importRow(row)
        except Exception as e: # Exceptions are caught so the keyMap can be saved
            self.saveKeyMap()
            raise e
        
        self.saveKeyMap()

