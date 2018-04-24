# configValidator: parses a configuration to validate and construct a postgres
# connection string
#
# Config: a configparser.ConfigParser() object
# SectionName: A string indicating which section of the configuration to generate a connection string from

def configValidator(Config, SectionName):
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
