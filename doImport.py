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


if __name__== "__main__":
    manager = php2django.ImportManager()
    manager.build_lookup_table([
         importAccounts.ImportUser,
         importAccounts.ImportTrainee,
         importAccounts.ImportTrainingAssistant,
         importTerms.ImportTerm,
    ],skip_if_pickle=True)
    manager.build_lookup_table([
         importAputils.ImportVehicle
    ])
    manager.process_imports(mock=False)