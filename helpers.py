#
# Helper functions
#
import os

# prettyprint: Prints a list in a sane way (tabbed out once, one element per line)
#
# list: a list of strings
#

def prettyprint(list):
    for i in list: print("\t %s" % i)

# wprint: A wrapped print function for use in a worker process. If the program
# is in debug mode, this will cause the process pid to be prepended to each string
#
# text: a string to be printed
# debug: a boolean indicating whether the program is in debug mode

def wprint(text, debug):
    if debug:
        print("%d: %s" % (os.getpid(), text))
    else:
        print(text)

# getCount: retrieves the number of rows in a given table from our memory backed sqlite3 database
#
# table_name: a string indicating the name of the table to be counted
# conf_name: which varient of the database we want to query
# database: a sqlite3.connect() object

def getCount(table_name, conf_name, database):
    mem_db = database.cursor()
    mem_db.execute("SELECT count(*) FROM %s" % (table_name + "_" + conf_name)) # I know this isn't the safe way to do this, but it isn't user facing code
    val = mem_db.fetchone()[0]
    mem_db.close()
    return val
