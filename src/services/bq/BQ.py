import os
import sys

from google.cloud import bigquery
from google.cloud.exceptions import BadRequest


# sys.path.append(os.path.dirname(os.path.dirname(
#     os.path.dirname(os.path.realpath(__file__)))))


class BQ():

    def __init__(self, client):
        self.client = client


class BQ_create_tables(BQ):

    def __init__(self, client):
        super().__init__(client)

    def create_bq_table(self,
                        project_id,
                        ds,
                        tb_name,
                        tb_scripts,
                        tb_filter_date_field,
                        is_partition_ds=False,
                        ):
        """

        :param project_id:
        :param ds:
        :param tb_name:
        :param tb_scripts:
        :param tb_filter_date_field:
        :param is_partition_ds:
        :return:
        """
        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = project_id + "." + ds + "." + tb_name

        schema = []
        # tb_scripts = json.loads(tb_scripts)
        for struct in tb_scripts:
            if struct["type"].strip("\'") == "NUMERIC":
                schema.append(
                    bigquery.SchemaField(
                        name=struct["name"].strip("\'"),
                        field_type="BIGNUMERIC",
                        mode=struct["mode"].strip("\'"),
                        precision=28+5,
                        scale=5
                    )
                )
            else:
                schema.append(
                    bigquery.SchemaField(
                        name=struct["name"].strip("\'"),
                        field_type=struct["type"].strip("\'"),
                        mode=struct["mode"].strip("\'")
                    )
                )
        table = bigquery.Table(table_id, schema=schema)
        if tb_filter_date_field != "" and is_partition_ds:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=tb_filter_date_field.strip(),
                require_partition_filter=False
            )
            print(table.time_partitioning)

        print(table)
        print("Create table {} ...".format(table_id))
        try:
            # Make an API request.
            table = self.client.create_table(table=table, exists_ok=False)
            print(
                "Created table {}.{}.{} done!".format(
                    table.project, table.dataset_id, table.table_id)
            )
        except Exception as e:
            print(e)

    def create_tables_for_ds(self,
                             project_id,
                             ds,
                             scripts,
                             key_target_script,
                             key_filter_date_field,
                             is_partition_ds):
        for tb_name in scripts:
            self.create_bq_table(
                project_id=project_id,
                ds=ds,
                tb_name=tb_name,
                tb_scripts=scripts[tb_name][key_target_script],
                tb_filter_date_field=scripts[tb_name][key_filter_date_field],
                is_partition_ds=is_partition_ds
            )


class BQ_delete_tables(BQ):

    def __init__(self, client):
        super().__init__(client)

    def delete_bq_table(self,
                        project_id,
                        ds,
                        tb_name):
        """

        :param client_:
        :param tb_name:
        :param ds:
        :return:
        """
        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = project_id + "." + ds + "." + tb_name

        table = bigquery.Table(table_id)
        print("Deleted table {}.{}.{}...".format(
            table.project, table.dataset_id, table.table_id))
        try:
            self.client.delete_table(table)  # Make an API request.
            print(
                "Deleted table {}.{}.{} done!".format(
                    table.project, table.dataset_id, table.table_id)
            )
        except Exception as e:
            print(e)

    def delete_bq_table_from_ds(self,
                                project_id,
                                ds,
                                tbs):
        for tb in tbs:
            self.delete_bq_table(project_id=project_id,
                                 ds=ds,
                                 tb_name=tb)


class BQ_loading_tables(BQ):

    def __init__(self, client):
        super().__init__(client)

    def insert_bq_table(self,
                        tb_name,
                        dataset_id,
                        project_id,
                        filepath,
                        delimeter,
                        is_overwrite=True):

        dataset_ref = self.client.dataset(dataset_id, project_id)
        table_ref = dataset_ref.table(tb_name)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.field_delimiter = delimeter
        # job_config.quote_character = '"'
        # job_config.encoding = "ISO-8859-1"
        job_config.allow_quoted_newlines = True
        job_config.max_bad_records = 1000000
        if is_overwrite:
            job_config.write_disposition = "WRITE_TRUNCATE"
        else:
            job_config.write_disposition = "WRITE_APPEND"
        job_config.autodetect = False

        job = None
        try:
            print(tb_name)
            with open(filepath, "r+b") as source_file:
                print("load job...")
                job = self.client.load_table_from_file(
                    source_file, table_ref, job_config=job_config)

            job.result()  # Waits for table load to complete.
            print("Loaded {} rows into {}:{}.".format(
                job.output_rows, dataset_id, tb_name))
            if job.output_rows == 0:
                print("Load 0 row!!!!!!!!!!!!!!!")
        except BadRequest as e:
            print(e)
            if job is not None:
                for e in job.errors:
                    print('{}'.format(e['message']))


class BQShare(BQ):

    def __init__(self, client):
        super().__init__(client)

    def authorize_view_to_access_dataset(
        self,
        project_id,
        source_dataset_ids,
        shared_dataset_ids,
        shared_view_ids
    ):
        for source_dataset_id in source_dataset_ids:
            print(f"Authorize to access dataset {source_dataset_id} ...")
            source_dataset_full_path = "{}.{}".format(
                project_id, source_dataset_id)
            source_dataset = bigquery.Dataset(source_dataset_full_path)

            access_entries = list(source_dataset.access_entries)

            for shared_dataset_id, shared_view_id in zip(shared_dataset_ids, shared_view_ids):
                # print(f"Authorize {shared_dataset_id}.{shared_view_id} to access dataset {source_dataset_id} ...")

                shared_dataset_full_path = "{}.{}".format(
                    project_id, shared_dataset_id)
                shared_dataset = bigquery.Dataset(shared_dataset_full_path)

                view = bigquery.Table(shared_dataset.table(shared_view_id))
                # print(access_entries)
                access_entries.append(
                    bigquery.AccessEntry(
                        None, "view", view.reference.to_api_repr())
                )
            # print(access_entries)

            try:
                source_dataset.access_entries = access_entries
                source_dataset = self.client.update_dataset(
                    source_dataset, ["access_entries"]
                )  # API request

                print(
                    f"Authorize to access dataset {source_dataset_id} DONE\n")
            except BadRequest as e:
                print(e)

            # print(source_dataset_id)
            # return source_dataset

    def call_query(self, query):
        try:
            print("-- Call query {} ...".format(query))

            query_job = self.client.query(query)  # Make an API request.

            results = query_job.result()  # Waits for job to complete.

            print("-- DONE CALL QUERY")
            return results
        except BadRequest as e:
            print(e)
            # if results is not None:
            #     for err in results.errors:
            #         print('{}'.format(err['message']))

            return "Error in query"


class BQ_get_table(BQ):

    def __init__(self, client):
        super().__init__(client)

    def get_bq_table(self,
                     project_id,
                     ds,
                     tb_name):
        """

        :param client_:
        :param tb_name:
        :param ds:
        :return:
        """
        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = project_id + "." + ds + "." + tb_name

        try:
            # View table properties
            table = self.client.get_table(table_id)
            print(
                "- Query table '{}.{}.{}'.".format(
                    table.project, table.dataset_id, table.table_id)
            )
            # print("--- Table schema: {}".format(table.schema))
            print("- Table description: {}".format(table.description))
            print("- Table has {} rows".format(table.num_rows))

            # Start the query, passing in the extra configuration.
            query_str = "SELECT DAY, CHANNEL_CODE, PLANT, REGION, PRODUCT_NAME, BASIC13, L2, L7, MATERIAL_GROUP, Q10, DT_VANG, NET_AMT FROM `pnj-sc-aa.INPUT_FC_ACTUAL_SALES.FC_D_ACTSALES_2022` LIMIT 50000;"
            query_job = self.client.query(
                query_str
            )  # Make an API request.
            rows = query_job.result().to_dataframe(create_bqstorage_client=False)
            df_result = rows.values.tolist()

            return df_result
        except Exception as e:
            print(e)
