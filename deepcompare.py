#!/usr/bin/python3
#
# Compare two databases to ensure data integrity
# Copyright 2018 PatientsLikeMe Inc.
# logan.brown@patientslikeme.com

# Imports
import psycopg2
import configparser
import sqlite3
import os
import signal
import sys
from dbworker import dbWorker
from multiprocessing import Process, Manager, Value
from helpers import prettyprint, getCount
from configValidator import configValidator


# Read in the task definition (this needs future work to allow commandline flags)
Config = configparser.ConfigParser()
Config.read('task.cfg')
debug = Config.getboolean("Global", "debug")
contest = Config.getboolean("Global", "connection_test")


# Globals so we can terminate our child processes on SIGINT
truth_proc = 0
test_proc = 0


#
# Self explainatory
#

def main():
    # Pull in the globals
    global truth_proc
    global test_proc


    # Preparations: Get the list of tables from each database and compare them.
    # If the list of tables doesn't match, bail out. THis also serves as a test
    # to ensure we can connect to the configured databases
    #

    truth_string = configValidator(Config, "Truth")
    test_string = configValidator(Config, "Test")
    try:
        truth_db = psycopg2.connect(truth_string)
        truth_cur = truth_db.cursor()
        print("Connected successfully to the Truth database!")
    except Exception as e:
        print("Unable to establish connection to the Truth database")
        print(e)
        exit()

    try:
        test_db = psycopg2.connect(test_string)
        test_cur = test_db.cursor()
        print("Connected successfully to the Test database!")
    except:
        print("Unable to establish connection to the Test database")
        exit()
    if contest:
        print("Both Connections established... Go time!")
        exit()

    truth_database = sqlite3.connect("file:/dev/shm/deepcompare_%s" % "truth", uri=True)
    truth_database.isolation_level = None
    test_database = sqlite3.connect("file:/dev/shm/deepcompare_%s" % "test", uri=True)
    test_database.isolation_level = None

    truth_cur.execute("select table_name,column_name from information_schema.columns where data_type='character varying' and character_maximum_length is not null and table_schema='public';")
    truth_table_list = truth_cur.fetchall()
    truth_cur.close()
    truth_db.close()

    test_cur.execute("select table_name,column_name from information_schema.columns where data_type='character varying' and character_maximum_length is not null and table_schema='public';")
    test_table_list = test_cur.fetchall()
    test_cur.close()
    test_db.close()

    '''
    if truth_table_list != test_table_list:
        print("ERROR: Databases do not contain the same tables. You're on your own...")
        if len(truth_table_list) > len(test_table_list):
            print("Truth database has additional rows:")
            prettyprint(list(set(truth_table_list)-set(test_table_list)))
        else:
            print("Test database has additional rows:")
            prettyprint(list(set(test_table_list)-set(truth_table_list)))
        exit()
    '''

    # Setting up manager variables: these are used if a table does not contain a
    # primary key. The row count is stored in these. Additionally they are cast to
    # a boolean to determine if the table is missing a primary key in the main loop

    manager = Manager()
    truth_rowonly = Value('i', 0)
    test_rowonly = Value('i', 0)


    # Main logic loop: for every table in the list that we previously determined
    # and validated, spin off two child processes to retrieve information about the
    # table from both databases (truth and test) and then compare the results.
    # First, we check if both tables have a primary key. If only one table has a
    # primary key, we abandon the table. Clearly something is wrong. If neither
    # table has a primary key, we compare the row counts only. If both tables have
    # primary keys, we validate the row count and then compare the md5 hash of each
    # row in the test database to the truth database.
    #
    for table in truth_table_list:
        print("Validating table %s:%s:" % table)
        truth_rowonly.value = 0
        test_rowonly.value = 0
        truth_proc = Process(target=dbWorker, args=("truth", truth_string, table, truth_rowonly, debug))
        truth_proc.start()
        test_proc = Process(target=dbWorker, args=("test", test_string, table, test_rowonly, debug))
        test_proc.start()
        truth_proc.join()
        test_proc.join()

        if bool(truth_rowonly.value) != bool(test_rowonly.value):
            print("\t One of these things is not like the other....")
            print("\t Primary key mismatch: something is very very wrong")
            sleep(3)
            continue
        if bool(truth_rowonly.value):
            print("\t NO PRIMARY KEY, only running row count validation")
            if truth_rowonly.value != test_rowonly.value:
                print("\t Row count ERROR! (%d vs. %d)" % (truth_values.len(), test_values.len()))
            else:
                print("\t Row count OK")
        else:
            if getCount(table, "test", test_database) != getCount(table, "truth", truth_database):
                print("\t Row count ERROR!")
            else:
                print("\t Row count OK")
            error = 0
            mem_true = truth_database.cursor()
            mem_true.execute("SELECT * from %s order by PKey" % (table[0] + "_" + table[1] + "_" + "truth"))
            mem_test = test_database.cursor()
            mem_test.execute("SELECT * from %s order by PKey" % (table[0] + "_" + table[1] + "_" + "test"))
            while True:
                true_val = mem_true.fetchone()
                test_val = mem_test.fetchone()
                if test_val == None or true_val == None:
                    break
                if (test_val[0] != true_val[0]) or (test_val[1] != true_val[1]):
                    error += 1
                    print("\t Hash ERROR! (PK ID: %s)" % true_val[0])
            if error:
                print("\t Hash count complete (%d errors)" % error)
            else:
                print("\tHash comparison OK")

            mem_true.execute("DROP TABLE %s" % (table[0] + "_" + table[1] + "_" + "truth"))
            mem_test.execute("DROP TABLE %s" % (table[0] + "_" + table[1] + "_" + "test"))
            mem_true.close()
            mem_test.close()


# graceful_shutdown: Gracefully terminate our child processes and remove the
# sqlite database stored in /dev/shm so the task can be run again without issue
#

def graceful_shutdown(signal, frame):
        print('Terminating gracefully...')
        test_proc.terminate()
        truth_proc.terminate()
        os.remove('/dev/shm/deepcompare')
        sys.exit(0)

# Glue logic to start the main function and remove the sqlite database on successful
# completion

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    main()
    os.remove('/dev/shm/deepcompare')
