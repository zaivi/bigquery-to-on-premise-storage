import os
import datetime
import pandas as pd


from src.utils.functions import get_query
from src.utils.supporter import parse_arg, print_args_info

from src.services.gcs.utils import download_bucket_objects

from src.services.bq.BQ import BQShare
from src.services.bq.utils import create_bq_connector_full_access

from src.services.oracle.utils import build_connection_input
from src.services.oracle.functions import start_loading_db

os.environ["GCLOUD_PROJECT"] = ""


def main():
    print("--- Starting")
    start = datetime.datetime.now().replace(microsecond=0)

    # Project information
    PROJECT_ID = ""
    DATASET_ID = ""
    GCS_BUCKET = ""

    # Parse argument
    arg_dict = parse_arg()
    print_args_info(arg_dict)

    # Initialize connector
    bq_client = create_bq_connector_full_access()
    bq_get_object = BQShare(bq_client)

    # Get extract query table
    query_info = [get_query(arg_dict['mDate'] ,PROJECT_ID, DATASET_ID, table_id) for table_id in arg_dict["mTable"]]

    # Dump file from table to GCS
    for (table_name, query, folder, _) in query_info:
        results = bq_get_object.call_query(
            query)
        table_id = PROJECT_ID + "." +  DATASET_ID + "." +  table_name
        table = bq_client.get_table(table_id)  # Make an API request.
        table_schema = table.schema
        download_bucket_objects(GCS_BUCKET, folder, "data/")

    # # Oracle db
    conn_profiles = build_connection_input()
    start_loading_db(conn_profiles, query_info, table_schema)

    end = datetime.datetime.now().replace(microsecond=0)
    print("--- Take time: ", end-start)


if __name__ == "__main__":
    main()
