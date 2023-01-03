import time
import datetime
import calendar

from src.utils.utils import id_generator



def get_query(
    date,
    project_id,
    dataset_id,
    table_id, 
    bucket="retrieve-files-from-bq", 
    delimiter="','", 
    format="CSV", 
    is_overwrite="true",
    is_header="true"):

    # Get id for dumping folder to GCS
    filter_id = id_generator(size=6)
    current_GMT = time.gmtime()
    time_stamp = calendar.timegm(current_GMT)
    
    folder = filter_id + '-' + str(time_stamp)

    datee = datetime.datetime.strptime(date, "%Y-%m-%d")

    if table_id == "PL_DATA_DETAIL_T5_T6":
        query = f"""
        EXPORT DATA
        OPTIONS (
            uri = 'gs://{bucket}/{folder}/{table_id}_*.csv',
            format = {format},
            overwrite = {is_overwrite},
            header = {is_header},
            field_delimiter = {delimiter})
        AS (
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE EXTRACT(MONTH FROM TRN_DATE) = {datee.month} and EXTRACT(YEAR FROM TRN_DATE) = {datee.year});
        """
    else:
        query = f"""
        EXPORT DATA
        OPTIONS (
            uri = 'gs://{bucket}/{folder}/{table_id}_*.csv',
            format = {format},
            overwrite = {is_overwrite},
            header = {is_header},
            field_delimiter = {delimiter})
        AS (
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`);
        """

    return table_id, query, folder, datee