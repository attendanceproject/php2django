#!/usr/bin/python

import os
import re
import sys

from datetime import datetime

if __name__== "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))

from php2django import php2django

import importAccounts


if __name__== "__main__":
    manager = php2django.ImportManager()
    manager.build_lookup_table([importAccounts.ImportUser])
    manager.process_imports()