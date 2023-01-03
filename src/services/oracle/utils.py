import os
import json
import cx_Oracle

def load_json_file(file_name, config_folder):
    """
        function to json file
        :arg:
            file_name: str.
            config_folder: str.
        :return:
            data: object.
    """
    full_path = os.path.join(config_folder, file_name)
    with open(full_path) as json_file:
        data = json.load(json_file)
    return data


def build_connection_input():
    """
        function to build migration profile
        :arg:
            args: object.
        :return:
            migration_profiles: list.
    """
    connection_profiles = []
    project_folder = "src/"
    config_folder = "config"
    config_folder = project_folder + config_folder
    config_file = "database_config.json"
    # load database config file
    lst_conn_db = load_json_file(file_name=config_file,
                                 config_folder=config_folder)
    for conn in lst_conn_db["list_datasource"]:
        lst_conn_parsed = parse_database_config(conn)
        connection_profiles.append(lst_conn_parsed)

    return connection_profiles


def parse_database_config(conn):
    datasource = conn["datasource_info"]
    connection_str = "oracle+cx_oracle://{0}:{1}@{2}:{3}/?service_name={4}". \
        format(datasource["user_name"],
               datasource["password"],
               datasource["hostname"],
               datasource["port"],
               datasource["sid"])

    return {"connection_str": connection_str, "tables": conn["tables"]}