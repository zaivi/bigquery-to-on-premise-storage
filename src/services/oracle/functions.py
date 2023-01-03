import pandas as pd

from src.services.oracle import oracle_connector


def start_loading_db(conn_profiles, query_info, table_schema):
    for conn_profile in conn_profiles:
        connection_str = conn_profile["connection_str"]
        table_names = conn_profile["tables"]
        db_connection = oracle_connector.get_database_connection(
            connection_str=connection_str)
        for table_name in table_names:
            for (table_id, _, folder, mdate) in query_info:
                if table_name == table_id:
                    format_table_name = "BQ_" + table_name
                    # Delete data before inserting new data
                    if table_name != "PL_DATA_DETAIL_T5_T6":
                        print("-- Deleting table before import")
                        oracle_connector.del_table_data(conn=db_connection, 
                                                        table_name=format_table_name,
                                                        condition_str=mdate)
                    row = oracle_connector.get_table_total_row(cursor=db_connection, table_name=format_table_name)
                    print(f"-- Total rows of table {format_table_name}: {row}")
                    # Insert data
                    oracle_connector.insert_files_database(
                        connection_str=connection_str, 
                        table_name=format_table_name, 
                        table_schema=table_schema,
                        folder=folder)
                    row = oracle_connector.get_table_total_row(cursor=db_connection, table_name=format_table_name)
                    print(f"-- Total rows of table {format_table_name}: {row}")