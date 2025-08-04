import oracledb
from functions import read_input
import pandas as pd

def connect_to_DB():
    ip='scprep.c6bcujowuk77.us-east-1.rds.amazonaws.com'
    port=1521
    service_name='SCPREP'
    dsn_tns=oracledb.makedsn(ip,port,service_name=service_name)
    username='A155532'
    password='MO@se2345ed'
    oracel_connection=oracledb.connect(user=username,password=password,dsn=dsn_tns)
    cursor=oracel_connection.cursor()
    return cursor

def drop_table_if_exists(cursor, table_name):
    drop_sql = f"""
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
                RAISE;
            END IF;
    END;
    """
    cursor.execute(drop_sql)

def get_com_id():
    cursor=connect_to_DB()
    parts=read_input()
    drop_table_if_exists(cursor, 'sawy1_temp_table1')
    drop_table_if_exists(cursor, 'sawy1')
    drop_table_if_exists(cursor, 'sawy2')
    create_temp_table_query = '''
        CREATE GLOBAL TEMPORARY TABLE sawy1_temp_table1 (
        Parts VARCHAR2(200)
        ) ON COMMIT PRESERVE ROWS
    '''
    cursor.execute(create_temp_table_query)
    
    insert_query = 'INSERT INTO sawy1_temp_table1 VALUES (:1)'
    cursor.executemany(insert_query, [(part,) for part in parts])
    
    join_query = '''
        create table sawy1 as
        SELECT t1.com_id as com_ids  ,t2.Parts
        FROM cm.xlp_se_component t1
        INNER JOIN sawy1_temp_table1 t2 ON TO_CHAR(t1.com_partnum) = t2.Parts and t1.man_id=1332
    '''
    cursor.execute(join_query)

def export_mfg_data():
    cursor=connect_to_DB()
    export_query='''
        create table sawy2 as
        select * from supp_chain.sc_mfr_parts A 
        inner join sawy1 B on B.com_ids=A.com_id
    '''
    cursor.execute(export_query)


get_com_id()
   