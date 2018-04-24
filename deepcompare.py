#!/usr/bin/python3
#
# Compare two databases to ensure data integrity
#

# Imports
import psycopg2
import configparser
import sqlite3
import os
import signal
import sys
from multiprocessing import Process, Manager, Value


# Read in the task definition
Config = configparser.ConfigParser()
Config.read('task.cfg')
debug = Config.getboolean("Global", "debug")

# Globals so we can kill our kids
truth_proc = 0
test_proc = 0

def prettyprint(list):
    for i in list: print("\t " + i)

def config_validator(SectionName):
    params = {}
    if Config.has_section(SectionName):
        for parameter in ['db_name', 'db_user', 'db_pass', 'db_host']:
            if Config.has_option(SectionName, parameter):
                params[parameter] = Config.get(SectionName, parameter)
            else:
                print(parameter + " missing from " + SectionName + "configuration")
                params[parameter] = raw_input("Enter a " + parameter + ": ")
    else:
        print(SectionName + " section missing from configuration")
        for parameter in ['db_name', 'db_user', 'db_pass', 'db_host']:
            params[parameter] = raw_input("Enter a " + parameter + ": ")
    return "dbname='" + params['db_name'] + "' user='" + params['db_user'] + "' host='" + params['db_host'] + "' password='" + params['db_pass'] + "'"

def wprint(text):
    if debug:
        print("%d: %s" % (os.getpid(), text))
    else:
        print(text)
#
# The worker for our multiprocessing "thread"
#
def db_worker(conf_name, connection_string, table_name, row_only):
    if debug:
        wprint("Connecting to %s" % conf_name)
    try:
        db = psycopg2.connect(connection_string)
        cur = db.cursor()
    except Exception as e:
        wprint("Unable to establish connection to the " + conf_name + " database")
        wprint(e)
        exit()
    try:
        mem_db = sqlite3.connect("file:/dev/shm/deepcompare?cache=shared", check_same_thread=False, uri=True)
        mem_db.isolation_level = None
        mem_cur = mem_db.cursor()
    except Exception as e:
        wprint("Unable to establish connection to the in-memory database")
        wprint(e)
        exit()
    try:
        mem_cur.execute("CREATE TABLE %s (PKey, md5sum)" % (table_name + "_" + conf_name))
        mem_cur.execute("CREATE INDEX %s ON %s (PKey)" % (table_name + "_" + conf_name + "_idx", (table_name + "_" + conf_name)))
        mem_db.commit()
    except Exception as e:
        wprint("Unable to initialize tables in the in-memory database")
        wprint(e)
        exit()
    cur.execute("SELECT c.column_name FROM information_schema.key_column_usage AS c JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.table_name ='" + table_name + "' AND t.constraint_type = 'PRIMARY KEY';")
    try:
        primary_key = cur.fetchone()[0]
        cur.execute("select " + primary_key + ",md5(cast(tab.* as text)) from " + table_name + " tab;")
    except Exception as e:
        wprint("\t No PKey found")
        wprint(e)
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
        wprint("Unable to store in the in-memory database")
        wprint(e)
    cur.close()
    db.close()
    mem_cur.execute("SELECT COUNT(*) from %s" % (table_name + "_" + conf_name))
    wprint("Inserted %d rows into table" % mem_cur.fetchone()[0])
    mem_cur.close()
    mem_db.close()

def get_count(table_name, conf_name, database):
    mem_db = database.cursor()
    mem_db.execute("SELECT count(*) FROM %s" % (table_name + "_" + conf_name)) # I know this isn't the safe way to do this, but it isn't user facing code
    val = mem_db.fetchone()[0]
    mem_db.close()
    return val
#
# Let's be a program!
#
def main():
    #Handle our Globals
    global truth_proc
    global test_proc
    # Let's do some shit
    truth_string = config_validator("Truth")
    test_string = config_validator("Test")
    try:
        truth_db = psycopg2.connect(truth_string)
        truth_cur = truth_db.cursor()
    except:
        print("Unable to establish connection to the Truth database")
        exit()
    print("Connected successfully to the Truth database!")

    try:
        test_db = psycopg2.connect(config_validator("Test"))
        test_cur = test_db.cursor()
    except:
        print("Unable to establish connection to the Truth database")
        exit()
    print("Connected successfully to the Test database!")
    database = sqlite3.connect("file:/dev/shm/deepcompare?cache=shared", check_same_thread=False, uri=True)
    database.isolation_level = None

    truth_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    truth_table_list = list(map(lambda x: x[0], truth_cur.fetchall()))
    truth_table_list.sort()
    truth_cur.close()
    truth_db.close()

    test_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    test_table_list = list(map(lambda x: x[0], test_cur.fetchall()))
    test_table_list.sort()
    test_cur.close()
    test_db.close()

    if truth_table_list != test_table_list:
        print("ERROR: Databases do not contain the same tables. You're on your own...")
        if len(truth_table_list) > len(test_table_list):
            print("Truth database has additional rows:")
            prettyprint(list(set(truth_table_list)-set(test_table_list)))
        else:
            print("Test database has additional rows:")
            prettyprint(list(set(test_table_list)-set(truth_table_list)))
        exit()

    manager = Manager()
    truth_rowonly = Value('i', 0)
    test_rowonly = Value('i', 0)
    #print("Tables to verify: ",
    #for tab in truth_table_list:
    #    print(tab

    for table in truth_table_list:
        print("Validating table " + table + ":")
        truth_rowonly.value = 0
        test_rowonly.value = 0
        truth_proc = Process(target=db_worker, args=("truth", truth_string, table, truth_rowonly))
        truth_proc.start()
        test_proc = Process(target=db_worker, args=("test", test_string, table, test_rowonly))
        test_proc.start()
        truth_proc.join()
        test_proc.join()

        if bool(truth_rowonly.value) != bool(test_rowonly.value):
            print("\t One of these things is not like the other.... Godspeed")
            sleep(3)
            print("\t (Primary key mismatch)")
            continue
        if bool(truth_rowonly.value):
            print("\t NO PRIMARY KEY, only running row count validation")
            if truth_rowonly.value != test_rowonly.value:
                print("\t Row count ERROR! (" + truth_values.len() + " vs. " + test_values.len() + ")")
            else:
                print("\t Row count OK")
        else:
            if get_count(table, "test", database) != get_count(table, "truth", database):
                print("\t Row count ERROR!")
            else:
                print("\t Row count OK")
            error = 0
            mem_true = database.cursor()
            mem_true.execute("SELECT * from %s order by PKey" % (table + "_" + "truth"))
            mem_test = database.cursor()
            mem_test.execute("SELECT * from %s order by PKey" % (table + "_" + "test"))
            while True:
                true_val = mem_true.fetchone()
                test_val = mem_test.fetchone()
                if test_val == None or true_val == None:
                    break
                if (test_val[0] != true_val[0]) or (test_val[1] != true_val[1]):
                    error += 1
                    print("\t Hash ERROR! (PK ID: " + true_val[0] + ")")
            if error:
                print("\t Hash count complete (" + error + " errors)")
            else:
                print("\tHash comparison OK")
            mem_true.close()
            mem_test.close()

def graceful_shutdown(signal, frame):
        print('Terminating gracefully...')
        test_proc.terminate()
        truth_proc.terminate()
        os.remove('/dev/shm/deepcompare')
        sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    main()
    os.remove('/dev/shm/deepcompare')
