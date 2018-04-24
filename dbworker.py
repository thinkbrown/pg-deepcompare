#
# The worker for our multiprocessing "thread"
#
import psycopg2
import sqlite3
from helpers import wprint

def dbWorker(conf_name, connection_string, table_name, row_only, debug):
    if debug:
        wprint("Connecting to %s" % conf_name, debug)
    try:
        db = psycopg2.connect(connection_string)
        cur = db.cursor()
    except Exception as e:
        wprint("Unable to establish connection to the " + conf_name + " database", debug)
        wprint(e, debug)
        exit()
    try:
        mem_db = sqlite3.connect("file:/dev/shm/deepcompare?cache=shared", check_same_thread=False, uri=True)
        mem_db.isolation_level = None
        mem_cur = mem_db.cursor()
    except Exception as e:
        wprint("Unable to establish connection to the in-memory database", debug)
        wprint(e, debug)
        exit()
    try:
        mem_cur.execute("CREATE TABLE %s (PKey, md5sum)" % (table_name + "_" + conf_name))
        mem_cur.execute("CREATE INDEX %s ON %s (PKey)" % (table_name + "_" + conf_name + "_idx", (table_name + "_" + conf_name)))
        mem_db.commit()
    except Exception as e:
        wprint("Unable to initialize tables in the in-memory database", debug)
        wprint(e, debug)
        exit()
    cur.execute("SELECT c.column_name FROM information_schema.key_column_usage AS c JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.table_name ='" + table_name + "' AND t.constraint_type = 'PRIMARY KEY';")
    try:
        primary_key = cur.fetchone()[0]
        cur.execute("select " + primary_key + ",md5(cast(tab.* as text)) from " + table_name + " tab;")
    except Exception as e:
        wprint("\t No PKey found", debug)
        wprint(e, debug)
        cur.execute("select count(*) from " + table_name + " tab;")
        row_only.value = int(cur.fetchone()[0])
        cur.close()
        db.close()
        mem_cur.close()
        mem_db.close()
        exit()
    try:
        while True:
            row = cur.fetchone()
            if row == None:
                break
            mem_cur.execute("INSERT INTO %s VALUES ('%s', '%s')" % (table_name + "_" + conf_name, row[0], row[1]))
            mem_db.commit()
    except Exception as e:
        wprint("Unable to store in the in-memory database", debug)
        wprint(e, debug)
    cur.close()
    db.close()
    mem_cur.execute("SELECT COUNT(*) from %s" % (table_name + "_" + conf_name))
    wprint("Inserted %d rows into table" % mem_cur.fetchone()[0], debug)
    mem_cur.close()
    mem_db.close()
