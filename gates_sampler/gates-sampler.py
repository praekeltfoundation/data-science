import pandas as pd
import sqlalchemy 
from sqlalchemy.sql import text
import numpy as np
from bokeh.io import output_file, show
from bokeh.models import GeoJSONDataSource
from bokeh.plotting import figure
from bokeh.sampledata.sample_geojson import geojson
import psycopg2
import time



def read_test_data(file_to_read):
    import pandas as pd
    test_data = pd.read_csv(file_to_read)
    return test_data

def connect_to_postgres(connection):
     engine = sqlalchemy.create_engine(connection)
     return engine

def write_master_db_to_postgres(master_db,table_name,engine):
    master_db.to_sql(table_name,engine)

def create_full_sample_temp_table(wave_id,group_id,conn_db):
    conn = conn_db.cursor() #create cursor to execute the direct db commands
    temp_full_sample_table_name = "temp_wave_%d"%(wave_id) 
    temp_table_name = "group_"+str(group_id)
    sql_execution = """DROP TABLE IF EXISTS %s;"""%(temp_full_sample_table_name)
    result = conn.execute(sql_execution)
    sql_execution = """CREATE TEMP TABLE  %s (LIKE %s);"""%(temp_full_sample_table_name,temp_table_name)
    print(sql_execution)
    result = conn.execute(sql_execution)
    conn.close()


def append_group_sample(wave_id,group_id,conn_db):
    conn = conn_db.cursor() #create cursor to execute the direct db commands
    temp_full_sample_table_name = "temp_wave_%d"%(wave_id)
    temp_table_name = "group_"+str(group_id)
    time.sleep(1)
    sql_execution = """insert into %s select * from %s;"""%(temp_full_sample_table_name,temp_table_name)
    print(sql_execution)
    result = conn.execute(sql_execution)
    conn.close()
    return(result)

def get_group_sample(group_id,conn_db,parent_table_name,population_filter,group_filter,group_value,samples):
    conn = conn_db.cursor() #create cursor to execute the direct db commands
    over psycopg2
    temp_table_name = "group_"+str(group_id)
    sql_execution = """DROP TABLE IF EXISTS %s;"""%(temp_table_name)
    result = conn.execute(sql_execution)
    sql_execution = """
    CREATE TABLE %s as (
            select *,random() as random 
            from %s 
            where %s 
            and %s in ('%s') 
            order by random asc 
            limit %d);
    """%(temp_table_name,parent_table_name,population_filter,group_filter,group_value,samples)
    print(sql_execution)
    result = conn.execute(sql_execution)
    sql_execution_count = """
            select count(*)
            from %s
            where %s 
            and %s in ('%s');
            """%(parent_table_name,population_filter,group_filter,group_value)
    print(sql_execution_count)
    count = conn.execute(sql_execution_count)
    print(group_filter,group_value,count)
    sample_count = pd.read_sql("""
            select count(*) 
            from %s;
            """%(temp_table_name),conn_db)
    full_count = pd.read_sql("""
            select count(*) 
            from %s 
            where %s 
            and %s in ('%s')
            """%(parent_table_name,population_filter,group_filter,group_value),conn_db)
    sample = pd.read_sql("""
            select * from %s;
            """%(temp_table_name),conn_db)
    print(sample_count['count'][0], samples)
    conn.close()
    if sample_count['count'][0] < samples:
         # Create a new instance of an exception
         print('error')
         sample_error = ValueError("The requested sample from %s is too large"%(group_value))
         raise sample_error
    return(sample_count,full_count,sample)

def main():

    #test configuration variables
    master_db_name = "postgres" #this will be added as part of the
    database_port = '6432'
    database_server = 'localhost'
    user = 'postgres'
    pwd = 'postgres'
    #configuration script
    id_column = "MSISDN" # column that is used for the unique identifier
    investigation = 1 # Investigation number tag
    wave = 1 # Wave tag
    filter = [""""EDD" between '2016-03-03' and '2016-03-10'""",
            """"province" in ('ec Eastern Cape Province',
            'kz KwaZulu-Natal Province',
            'nw North West Province',
            'nc Northern Cape Province',
            'wc Western Cape Province')"""]
    splitByColumn = "Language"
    splitValues = ["afr_ZA","eng_ZA"]
    ###############
    
    conn_db=psycopg2.connect(dbname=master_db_name,user=user,password=pwd,
            port=database_port, host=database_server)
    population_filter = """
    Province in (
                    'Eastern Cape Province',
                    'KwaZulu-Natal Province',
                    'North West Province',
                    'Western Cape Province',
                    'Northern Cape Province')
                and "Facility.code" not in ('Unavailable')
                and "EDD" between '2015-12-01' and '2016-06-01'
                """
    group_filter = "\"Language\""
    group_values = ['afr_ZA','eng_ZA','xho_ZA']
    group_sample_number = [200,200,250]
    parent_table_name = "ecd_full_sample_data_set_deduped"
    
    #for loop
    for i in range(0,len(group_values)):
        (a,b,df) = get_group_sample(i,conn_db,parent_table_name,population_filter,group_filter,group_values[i],group_sample_number[i])
        print(i)
    #create the temp DB to hold all the samples together    
    retVal = create_full_sample_temp_table(wave,0,conn_db)
    
    #append the individual samples into the new temp DB
    for i in range(0,len(group_values)):
        retVal = append_group_sample(wave,i,conn_db)

    sample = pd.read_sql("""select * from temp_wave_1""",conn_db)


    output_file("stocks.html", title="stocks.py example")
    sample = engine.execute(text("""insert into temp_wave_1 select * from
        group_0""").execution_options(autocommit=true))
  count = engine.execute(
                          text(sql_execution_count).execution_options(autocommit=True))

