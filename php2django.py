#!/usr/bin/python

import MySQLdb
import os
import pickle
import re
import sys
import traceback
import types

#You should create a local_settings.py file for your django settings
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
        #try:
        param = {}
        print row
        for prop in self.mapping.__dict__:
            if not ignore.match(prop):
                var = self.mapping.__dict__[prop]
                if isinstance(var,types.FunctionType):
                    param[prop]=var(self.mapping,row)
                else:
                    param[prop]=row[var]
        print param
        modelInstance = self.model.objects.create(**param)
        modelInstance.save()
        if not self.key is None:
            if isinstance(self.key,types.FunctionType):
                key = self.key(row)
            else:
                key = row[self.key]
            self.keyMap[key]=modelInstance.pk
        print modelInstance
        #except Exception, exp:
        #    print exp
    
    def doImport(self):
        cur.execute(self.query)
        result = cur.fetchall()
        try:
            for row in result:
                if self.rowFilter(row):
                    self.importRow(row)
        except Exception:
            traceback.print_exc()
            
        self.saveKeyMap()

