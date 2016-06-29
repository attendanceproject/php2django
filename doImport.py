#!/usr/bin/python

import os
import re
import sys

from datetime import datetime

import php2django
print sys.path

import importAccounts
import importAputils
import importTerms
import importTeams



if __name__== "__main__":
    manager = php2django.ImportManager()
    manager.build_lookup_table([
        importTerms.ImportTerm
    ],skip_if_pickle=True)
    manager.build_lookup_table([
        importTeams.ImportTeam
    ],skip_if_pickle=True)
    manager.build_lookup_table([
         importAccounts.ImportUser
    ],skip_if_pickle=True)
    manager.build_lookup_table([
         importAputils.ImportVehicle
    ],skip_if_pickle=True)

    manager.process_imports(mock=False)

    manager.process_biblereading_import()
    manager.process_biblebooks_import()