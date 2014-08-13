# php2django

This is meant to be a one-time-use set of code to migrate the data from the
mysql database from the old attendance project to djattendance.
Some code may be / become useful for general data migration in general.

Class definitions are needed for each of djattendance models which specify a
mysql query using the old database schema and a list of fields to match up.
Functions can be included in the mapping which are given all the data from a
corresponding row in the query set to use in generating the new field value.