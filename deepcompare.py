#!/usr/bin/python
#
# Compare two databases to ensure data integrity
#

# Imports
import psycopg2
import ConfigParser
from multiprocessing import Process, Manager, Value
# Read in the task definition
Config = ConfigParser.ConfigParser()
Config.read('task.cfg')

def config_validator(SectionName):
    params = {}
    if Config.has_section(SectionName):
        for parameter in ['db_name', 'db_user', 'db_pass', 'db_host']:
            if Config.has_option(SectionName, parameter):
                params[parameter] = Config.get(SectionName, parameter)
            else:
                print parameter + " missing from " + SectionName + "configuration"
                params[parameter] = raw_input("Enter a " + parameter + ": ")
    else:
        print SectionName + " section missing from configuration"
        for parameter in ['db_name', 'db_user', 'db_pass', 'db_host']:
            params[parameter] = raw_input("Enter a " + parameter + ": ")
    return "dbname='" + params['db_name'] + "' user='" + params['db_user'] + "' host='" + params['db_host'] + "' password='" + params['db_pass'] + "'"


#
# The worker for our multiprocessing "thread"
#
def db_worker(conf_name, connection_string, table_name, output, row_only):
    try:
        db = psycopg2.connect(connection_string)
        cur = db.cursor()
    except:
        print "Unable to establish connection to the " + conf_name + " database"
        exit()
    cur.execute("SELECT c.column_name FROM information_schema.key_column_usage AS c JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.table_name ='" + table_name + "' AND t.constraint_type = 'PRIMARY KEY';")
    try:
        primary_key = cur.fetchone()[0]
        cur.execute("select " + primary_key + ",md5(cast(tab.* as text)) from " + table_name + " tab;")
        output = dict(cur.fetchall())
        cur.close()
        db.close()
    except:
        cur.execute("select count(*) from " + table_name + " tab;")
        row_only.value = cur.fetchone()
        cur.close()
        db.close()

#
# Let's be a program!
#
def main():
# Let's do some shit
    truth_string = config_validator("Truth")
    test_string = config_validator("Test")
    try:
        truth_db = psycopg2.connect(truth_string)
        truth_cur = truth_db.cursor()
    except:
        print "Unable to establish connection to the Truth database"
        exit()
    print "Connected successfully to the Truth database!"

    try:
        test_db = psycopg2.connect(config_validator("Test"))
        test_cur = test_db.cursor()
    except:
        print "Unable to establish connection to the Truth database"
        exit()
    print "Connected successfully to the Test database!"

    truth_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    truth_table_list = map(lambda x: x[0], truth_cur.fetchall())
    truth_table_list.sort()
    truth_cur.close()
    truth_db.close()

    test_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    test_table_list = map(lambda x: x[0], test_cur.fetchall())
    test_table_list.sort()
    test_cur.close()
    test_db.close()

    if truth_table_list != test_table_list:
        print "ERROR: Databases do not contain the same tables. I'm out..."
        exit()

    manager = Manager()
    truth_values = manager.dict()
    test_values = manager.dict()
    truth_rowonly = Value('i', 0)
    test_rowonly = Value('i', 0)
    #print "Tables to verify: ",
    #for tab in truth_table_list:
    #    print tab

    for table in truth_table_list:
        print "Validating table " + table + ":"
        truth_rowonly.value = 0
        test_rowonly.value = 0
        truth_proc = Process(target=db_worker, args=("truth", truth_string, table, truth_values, truth_rowonly))
        truth_proc.start()
        test_proc = Process(target=db_worker, args=("test", test_string, table, test_values, test_rowonly))
        test_proc.start()
        truth_proc.join()
        test_proc.join()

        if bool(truth_rowonly.value) != bool(test_rowonly.value):
            print "\t One of these things is not like the other.... Godspeed"
            sleep(3)
            print "\t (Primary key mismatch)"
            continue
        if bool(truth_rowonly.value):
            print "\t NO PRIMARY KEY, only running row count validation"
            if len(truth_rowonly.value) != len(test_rowonly.value):
                print "\t Row count ERROR! (" + truth_values.len() + " vs. " + test_values.len() + ")"
            else:
                print "\t Row count OK"
        else:
            loop_truth_values = dict(truth_values)
            loop_test_values = dict(test_values)

            if len(loop_truth_values) != len(loop_test_values):
                print "\t Row count ERROR! (" + truth_values.len() + " vs. " + test_values.len() + ")"
            else:
                print "\t Row count OK"
            error = 0
            for row in loop_truth_values:
                try:
                    if loop_truth_values[row] != loop_test_values[row]:
                        print "\t Hash ERROR! (PK ID: " + row + ")"
                        error += 1
                except:
                    print "\t Row Disparity (PK ID: " + row + ")"
            if error:
                print "\t Hash count complete (" + error + " errors)"
            else:
                print "\tHash comparison OK"

if __name__ == "__main__":
    main()
