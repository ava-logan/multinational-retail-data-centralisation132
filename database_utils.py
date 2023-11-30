import yaml 

class DatabaseConnector:
    #used to connect and upload data to database

    def read_db_creds():
    #read creds in yaml and return dictionary 
    creds_dict = {}
    for key, value in yaml.load(open('db_creds.yaml')).iteritems():
        creds_dict[key]=value
    return creds_dict    

    def init_db_engine():
        import sqlalchemy as sa 
        sa.engine.url.URL = sa.engine.URL.create(creds_dict)
    return sa.engine.url.URL

    #reads read_db_cred and initialise and returns an sqalchemy database engine 

    def list_db_tables():
    #useds engine from init_db_engine to list all tables 