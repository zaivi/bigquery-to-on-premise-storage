import datetime
import glob, os
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy import types

from src.utils.utils import delete_folder



def get_database_info(cursor):
    query = """
    SELECT table_schema AS 'database_name', 
    ROUND(SUM(data_length + index_length) / 1024 / 1024, 5) AS 'size' 
    FROM information_schema.TABLES 
    GROUP BY table_schema 
    ORDER BY size DESC 
    """
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results


def get_table_info(cursor, schema_name):
    query = """
    SELECT t1.* from
    (SELECT TABLE_NAME AS 'table', ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 5) AS 'size'
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = '{0}'
    ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC) t1
    where t1.size > 0
    """.format(schema_name)

    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results


def get_database_total_size(public_ip, username, password):
    if None not in (public_ip, username, password):
        connection = database_connection(public_ip, username, password)
        if connection is not None:
            cursor = connection.cursor()
            database_info = get_database_info(cursor)
            total_size = sum(c.get("size") for c in database_info)
            return float(total_size)
    return None


def get_list_tables(cursor, schema_name):
    query = """
    SELECT t1.table from
    (SELECT TABLE_NAME AS 'table', ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 5) AS 'size'
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = '{0}'
    ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC) t1
    where t1.size > 0
    """.format(schema_name)

    cursor.execute(query)
    results = [list(i) for i in cursor.fetchall()]
    return results


def get_table_checksum(cursor, schema_name, table_name):
    query = """
    checksum table {0}.{1}
    """.format(schema_name, table_name)
    cursor.execute(query)
    results = [list(i) for i in cursor.fetchall()]
    return results


def get_list_database(cursor):
    query = """
    SHOW databases
    """
    cursor.execute(query)
    results = [list(i) for i in cursor.fetchall()]
    list_database = []
    for database in results:
        database_name = database[0]
        if database_name != 'information_schema' and database_name != 'mysql' and database_name != 'performance_schema' \
                and database_name != 'sys':
            list_tables = get_list_all_tables(cursor, database_name)
            data = {
                'database_name': database_name,
                'tables': list_tables.get('tables')
            }
            list_database.append(data)

    final_data = {
        "databases": list_database
    }
    return final_data


def get_list_all_tables(cursor, schema_name):
    query = """
    SELECT TABLE_NAME AS 'table'
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = '{0}'
    """.format(schema_name)

    cursor.execute(query)
    results = [list(i) for i in cursor.fetchall()]
    list_table = []
    for table in results:
        list_table.append(table[0])
    final_data = {
        "tables": list_table
    }
    return final_data


def get_table_create_statement(cursor, table_name):
    query = """
    SHOW CREATE TABLE {0}
    """.format(table_name)

    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))

    split_table_name = table_name.split(".")

    create_string = """CREATE TABLE `{0}`""".format(split_table_name[1])
    replace_string = """CREATE TABLE IF NOT EXISTS {0}""".format(table_name)

    final_statement = results[0].get('Create Table').replace(
        create_string, replace_string, 1)
    return final_statement


def create_database_if_not_exists(cursor, schema_name):
    query = """
    CREATE DATABASE IF NOT EXISTS {0}
    """.format(schema_name)
    cursor.execute(query)


def create_table_by_statement(cursor, create_statement):
    cursor.execute(create_statement)


def get_table_total_row(cursor, table_name: str) -> int:
    query = """SELECT COUNT(*) FROM {0}""".format(table_name)
    result = cursor.execute(query)
    row = [item[0] for item in result.fetchall()]
    return row[0]


def get_table_total_row_with_condition(cursor, table_name: str, condition_str) -> int:
    query = """SELECT COUNT(*) FROM {0} {1}""".format(table_name,
                                                      condition_str)
    result = cursor.execute(query)
    row = [item[0] for item in result.fetchall()]
    return row[0]


def get_database_connection(connection_str):
    db_connection = sqlalchemy.create_engine(connection_str,
                                             pool_timeout=180,
                                             pool_recycle=90,
                                             pool_pre_ping=True)
    db_connection.dialect.server_side_cursors = True
    db_connection.execution_options(stream_results=True)
    return db_connection


def insert_files_database(connection_str, table_name, table_schema, folder):
    path = 'data/' + folder
    csv_files = sorted([f for f in os.listdir(path) if f.endswith('.csv')])
    print(f"Total files are importing: {len(csv_files)}")
    conn = get_database_connection(
            connection_str=connection_str)
    for index, csv_file in enumerate(csv_files):
        if index % 40 == 0:
            conn = get_database_connection(
            connection_str=connection_str)
            print("-- Created new connection to Oracle")

        print(f"-- Importing file: {csv_file}")
        full_path = path + '/' + csv_file
        
        df = pd.read_csv(full_path, dtype='unicode')
        dtyp = {c:types.VARCHAR(df[c].str.len().max())
                for c in df.columns[df.dtypes == 'object'].tolist()}
        df = df.replace(np.nan, "")
        
        for schema in table_schema:
            try:
                if schema.field_type == "DATE":
                    df[schema.name] = pd.to_datetime(df[schema.name], format="%Y-%m-%d")
            except Exception as e:
                pass
        df.to_sql(table_name, con = conn, if_exists = 'append', chunksize = 50000, index=False, dtype=dtyp)
        print(f"- Imported: {df.shape[0]} rows successfully")

    # Delete files after import to Oracle
    print(f"-- Removing folder: {path} after importing successfully to Database")
    delete_folder(path)


def del_table_data(conn, table_name: str, condition_str) -> int:
    if table_name == "PL_DATA_DETAIL_T5_T6":
        query = f"""DELETE FROM {table_name} WHERE EXTRACT(MONTH FROM TRN_DATE) = {condition_str.month} and EXTRACT(YEAR FROM TRN_DATE) = {condition_str.year}"""
    else:
        query = f"""TRUNCATE TABLE {table_name}"""
    result = conn.execute(query)
    return True