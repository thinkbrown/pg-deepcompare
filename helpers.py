#
# Helper functions
#
import os

def prettyprint(list):
    for i in list: print("\t " + i)

def wprint(text, debug):
    if debug:
        print("%d: %s" % (os.getpid(), text))
    else:
        print(text)

def getCount(table_name, conf_name, database):
    mem_db = database.cursor()
    mem_db.execute("SELECT count(*) FROM %s" % (table_name + "_" + conf_name)) # I know this isn't the safe way to do this, but it isn't user facing code
    val = mem_db.fetchone()[0]
    mem_db.close()
    return val
