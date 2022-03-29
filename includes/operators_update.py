from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
# from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException

# from lip_bw_extractor.utils_ds67 import get_query_change_column, get_query_get_column_info_bq
# from lip_bw_extractor.utils_ds67 import createDirectory, get_nick


from .utils_ds67 import get_query_change_column, get_query_get_column_info_bq
from .utils_ds67 import createDirectory, get_nick

from google.cloud import bigquery

from datetime import datetime
import subprocess
from tempfile import NamedTemporaryFile
from typing import List, Optional, Sequence, Union

import json
import sys
import os
import pandas as pd


class FCastFolderPickUpdaterOperator(BaseOperator):
    """Find the most current folder according to a certain date format .

    Args:
        :param bucket: GCP Storage bucket.
        :type bucket: str
        :param prefix_blob: GCP Storage blob where to find folder.
        :type prefix_blob: str
        :param pick_folder: If set `True` it will search the last folder, else it will select
            all GCP Storage blob. (Default: `True`)
        :type pick_folder: bool
        :param google_cloud_storage_conn_id: Reference to a specific GCP Storage hook.
            (Default: "google_cloud_default")
        :type google_cloud_storage_conn_id: str
        :param date_format_increment: Date format used to find the last folder. (Default: "%Y%m%d")
        :type date_format_increment: str
        :param delimiter: Suffix used to filter the files in folders. (Default: ".csv")
        :type delimiter: str

    """

    @apply_defaults
    def __init__(self,
                 bucket: str,
                 prefix_blob: str,
                 pick_folder: bool = True,
                 google_cloud_storage_conn_id: str = "google_cloud_default",
                 date_format_increment: str = "%Y%m%d",
                 delimiter: str = ".csv",
                 **kwargs) -> None:

        super().__init__(**kwargs)
        self.pick_folder = pick_folder
        self.prefix_blob = prefix_blob if prefix_blob.endswith("/") else prefix_blob + "/"
        self.bucket = bucket
        self.date_format_increment = date_format_increment
        self.delimiter = delimiter
        self._hook = GoogleCloudStorageHook(google_cloud_storage_conn_id)

    def _get_folder_list(self) -> List[str]:

        file_list = self._hook.list(bucket=self.bucket,
                                    prefix=self.prefix_blob,
                                    delimiter=self.delimiter)
        _list_aux = list()

        for file in file_list:
            name_aux = file.replace(self.prefix_blob, "")
            file_aux = name_aux.split("/")[-1]
            _list_aux.append(name_aux.replace("/"+file_aux, ""))

        return _list_aux

    def _folder_picker(self) -> List[str]:

        folder_list = self._get_folder_list()

        date_list = \
            [datetime.strptime(file, self.date_format_increment)
                for file in folder_list]

        right_folder = self.prefix_blob + \
            datetime.strftime(max(date_list), self.date_format_increment) + \
            "/*" + self.delimiter

        self.log.info("Trabajando en con los archivos del blob: {}".format(right_folder))

        return [right_folder]

    def _folder_all(self) -> List[str]:

        folder_list = self._get_folder_list()

        prefix_folder_list = list()

        for folder in folder_list:
            right_folder = self.prefix_blob+folder+"/*"+self.delimiter
            self.log.info("Trabajando en con los archivos del blob: {}".format(right_folder))
            prefix_folder_list.append(right_folder)

        return prefix_folder_list

    def _encode_str_folders(self, out_folder: list, code: str = "") -> str:
        str_out = ""
        for i, folder in enumerate(out_folder):
            if i == 0:
                str_out += "'{}'".format(folder)
            else:
                str_out += code+"'{}'".format(folder)

        return "[" + str_out + "]"

    def execute(self, context):
        if self.pick_folder:
            out_folder = self._folder_picker()
        else:
            out_folder = self._folder_all()
        self.log.info("out_folder " + str(out_folder))

        if self.pick_folder:
            config_write = {"create_disposition": "CREATE_IF_NEEDED",
                            "write_disposition": "WRITE_APPEND"}
        else:
            config_write = {"create_disposition": "CREATE_IF_NEEDED",
                            "write_disposition": "WRITE_TRUNCATE"}

        context["ti"].xcom_push(key="folders", value=out_folder)
        context["ti"].xcom_push(key="config_disposition", value=config_write)

        return None


class FCastFilePickUpdaterOperator(BaseOperator):
    """Find the most current file according to a certain date format .

    Args:
        :param bucket: GCP Storage bucket.
        :type bucket: str
        :param prefix_blob: GCP Storage blob where to find file.
        :type prefix_blob: str
        :param pick_folder: If set `True` it will search the last file, else it will select
            all GCP Storage blob. (Default: `True`)
        :type pick_folder: bool
        :param suffix: Suffix (no file extention) used to find the date in file name.
        :type suffix: str
        :param google_cloud_storage_conn_id: Reference to a specific GCP Storage hook.
            (Default: "google_cloud_default")
        :type google_cloud_storage_conn_id: str
        :param date_format_increment: Date format used to find the last file. (Default: "%Y%m%d")
        :type date_format_increment: str
        :param delimiter: Suffix used to filter the files. (Default: ".csv")
        :type delimiter: str

    """

    @apply_defaults
    def __init__(self,
                 bucket: str,
                 prefix_blob: str,
                 suffix: str,
                 pick_folder: bool = True,
                 size_prefix: int = 0,
                 google_cloud_storage_conn_id: str = "google_cloud_default",
                 date_format_increment: str = "%Y%m%d",
                 delimiter: str = ".csv",
                 **kwargs) -> None:

        super().__init__(**kwargs)
        self.pick_folder = pick_folder
        self.prefix_blob = prefix_blob
        self.size_prefix = size_prefix
        self.suffix = suffix
        self.bucket = bucket
        self.date_format_increment = date_format_increment
        self.delimiter = delimiter
        self._hook = GoogleCloudStorageHook(google_cloud_storage_conn_id)
        self._delimiter_aux = self.suffix + self.delimiter if not self.suffix.endswith(self.delimiter) else self.suffix

    def _get_file_list(self) -> List[str]:

        file_list = self._hook.list(bucket=self.bucket,
                                    prefix=self.prefix_blob,
                                    delimiter=self.delimiter)
        _list_aux = list()

        for file in file_list:
            if file.endswith("2021_0708.csv") or file.endswith("_20210708.csv") or file.endswith("_20210822.csv"):
                continue
            print(file)
            name_aux = file.replace(self.prefix_blob, "").replace(self._delimiter_aux, "")
            _list_aux.append(name_aux[self.size_prefix:])

        return _list_aux, file_list

    def _get_file_date(self, date_str, dir_list) -> str:

        self.log.info(f"prefix_blob: {self.prefix_blob}")
        self.log.info(f"size_prefix: {self.size_prefix}")

        for direc in dir_list:
            if date_str in direc.replace(self.prefix_blob, "")[self.size_prefix:]:
                return direc
            else:
                pass
        print("No se encontro el archivo")
        return None

    def _file_picker(self) -> List[str]:

        # string date list
        file_list, dir_list = self._get_file_list()

        self.log.info(f"date_format_increment: {self.date_format_increment}")

        # datetime date list
        date_list = \
            [datetime.strptime(file, self.date_format_increment)
                for file in file_list]

        self.log.info(f"date_list: {date_list}")

        date_max = datetime.strftime(max(date_list), self.date_format_increment)

        self.log.info(f"date_max: {date_max}")
        self.log.info(f"dir_list: {dir_list}")

        right_folder = self._get_file_date(date_max, dir_list)

        self.log.info("Trabajando en con los archivos del blob: {}".format(right_folder))

        return [right_folder]

    def _file_all(self) -> List[str]:

        file_list, dir_list = self._get_file_list()

        prefix_file_list = list()

        for file in dir_list:
            self.log.info("Trabajando en con los archivos del blob: {}".format(file))
            prefix_file_list.append(file)

        return prefix_file_list

    def _encode_str_folders(self, out_folder: list, code: str = "") -> str:
        str_out = ""
        for i, folder in enumerate(out_folder):
            if i == 0:
                str_out += "'{}'".format(folder)
            else:
                str_out += code+"'{}'".format(folder)

        return "[" + str_out + "]"

    def execute(self, context):
        if self.pick_folder:
            out_folder = self._file_picker()
        else:
            out_folder = self._file_all()
        self.log.info("out_folder " + str(out_folder))

        if self.pick_folder:
            config_write = {"create_disposition": "CREATE_IF_NEEDED",
                            "write_disposition": "WRITE_APPEND"}
        else:
            config_write = {"create_disposition": "CREATE_IF_NEEDED",
                            "write_disposition": "WRITE_TRUNCATE"}

        context["ti"].xcom_push(key="folders", value=out_folder)
        context["ti"].xcom_push(key="config_disposition", value=config_write)

        return None


class FCastCustomGCSToBQOperator(BaseOperator):

    """
    Loads files from Google cloud storage into BigQuery.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google cloud storage object name. The object in
    Google cloud storage must be a JSON file with the schema fields in it.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCloudStorageToBigQueryOperator`

    :param bucket: The bucket to load from. (templated)
    :type bucket: str
    :param source_objects: List of Google cloud storage URIs to load from. (templated)
        If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
    :type source_objects: list[str]
    :param destination_project_dataset_table: The dotted
        ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to load data into.
        If ``<project>`` is not included, project will be the project defined in
        the connection json. (templated)
    :type destination_project_dataset_table: str
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        Should not be set when source_format is 'DATASTORE_BACKUP'.
        Parameter must be defined if 'schema_object' is null and autodetect is False.
    :type schema_fields: list
    :param schema_object: If set, a GCS object path pointing to a .json file that
        contains the schema for the table. (templated)
        Parameter must be defined if 'schema_fields' is null and autodetect is False.
    :type schema_object: str
    :param source_format: File format to export.
    :type source_format: str
    :param compression: [Optional] The compression type of the data source.
        Possible values include GZIP and NONE.
        The default value is NONE.
        This setting is ignored for Google Cloud Bigtable,
        Google Cloud Datastore backups and Avro formats.
    :type compression: str
    :param create_disposition: The create disposition if the table doesn't exist.
    :type create_disposition: str
    :param skip_leading_rows: Number of rows to skip when loading from a CSV.
    :type skip_leading_rows: int
    :param write_disposition: The write disposition if the table already exists.
    :type write_disposition: str
    :param field_delimiter: The delimiter to use when loading from a CSV.
    :type field_delimiter: str
    :param max_bad_records: The maximum number of bad records that BigQuery can
        ignore when running the job.
    :type max_bad_records: int
    :param quote_character: The value that is used to quote data sections in a CSV file.
    :type quote_character: str
    :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
        extra values that are not represented in the table schema.
        If true, the extra values are ignored. If false, records with extra columns
        are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result.
    :type ignore_unknown_values: bool
    :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
    :type allow_quoted_newlines: bool
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :type allow_jagged_rows: bool
    :param encoding: The character encoding of the data. See:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).csvOptions.encoding
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
    :type encoding: str
    :param max_id_key: If set, the name of a column in the BigQuery table
        that's to be loaded. This will be used to select the MAX value from
        BigQuery after the load occurs. The results will be returned by the
        execute() command, which in turn gets stored in XCom for future
        operators to use. This can be helpful with incremental loads--during
        future executions, you can pick up from the max ID.
    :type max_id_key: str
    :param bigquery_conn_id: Reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: list
    :param src_fmt_configs: configure optional fields specific to the source format
    :type src_fmt_configs: dict
    :param external_table: Flag to specify if the destination table should be
        a BigQuery external table. Default Value is False.
    :type external_table: bool
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.
        Note that 'field' is not available in concurrency with
        dataset.table$partition.
    :type time_partitioning: dict
    :param cluster_fields: Request that the result of this load be stored sorted
        by one or more columns. This is only available in conjunction with
        time_partitioning. The order of columns given determines the sort order.
        Not applicable for external tables.
    :type cluster_fields: list[str]
    :param autodetect: [Optional] Indicates if we should automatically infer the
        options and schema for CSV and JSON sources. (Default: ``True``).
        Parameter must be setted to True if 'schema_fields' and 'schema_object' are undefined.
        It is suggested to set to True if table are create outside of Airflow.
    :type autodetect: bool
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    """

    template_fields = ('bucket', 'source_objects',
                       'schema_object', 'destination_project_dataset_table')

    template_ext = ('.sql',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 destination_project_dataset_table,
                 source_objects_task_id_key_list=None,
                 source_objects=None,
                 schema_fields=None,
                 schema_object=None,
                 source_format='CSV',
                 compression='NONE',
                 create_write_disposition_task_id_key_list=None,
                 create_disposition=None,
                 write_disposition=None,
                 skip_leading_rows=0,
                 field_delimiter=',',
                 max_bad_records=0,
                 quote_character=None,
                 ignore_unknown_values=False,
                 allow_quoted_newlines=False,
                 allow_jagged_rows=False,
                 encoding="UTF-8",
                 max_id_key=None,
                 bigquery_conn_id='bigquery_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 schema_update_options=(),
                 src_fmt_configs=None,
                 external_table=False,
                 time_partitioning=None,
                 cluster_fields=None,
                 autodetect=True,
                 encryption_configuration=None,
                 *args, **kwargs):

        super(FCastCustomGCSToBQOperator, self).__init__(*args, **kwargs)

        # GCS config
        if src_fmt_configs is None:
            src_fmt_configs = {}
        if time_partitioning is None:
            time_partitioning = {}
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        if self.source_objects is not None:
            self.source_objects_task_id = None
            self.source_objects_key = None
        else:
            self.source_objects_task_id = source_objects_task_id_key_list[0]
            self.source_objects_key = source_objects_task_id_key_list[1]

        # BQ config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.compression = compression

        # write options
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.create_write_disposition_task_id_key_list = create_write_disposition_task_id_key_list

        self.skip_leading_rows = skip_leading_rows
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.ignore_unknown_values = ignore_unknown_values
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.external_table = external_table
        self.encoding = encoding

        self.max_id_key = max_id_key
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.schema_update_options = schema_update_options
        self.src_fmt_configs = src_fmt_configs
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.autodetect = autodetect
        self.encryption_configuration = encryption_configuration

    def _get_write_disposition(self, task_instance):

        if self.create_write_disposition_task_id_key_list is None and \
                self.write_disposition is not None:
            _write_disposition = self.write_disposition
        elif self.create_write_disposition_task_id_key_list is not None and \
                self.write_disposition is None:

            task_id = self.create_write_disposition_task_id_key_list[0]
            key = self.create_write_disposition_task_id_key_list[1]

            _write_disposition = task_instance.xcom_pull(task_ids=task_id, key=key)["write_disposition"]
        else:
            self.log.error("[OPTION_WRITE] Parametros ingresados de forma invalida !")
            _write_disposition = None

        return _write_disposition

    def _get_create_disposition(self, task_instance):

        if self.create_write_disposition_task_id_key_list is None and \
                self.create_disposition is not None:
            _create_disposition = self.create_disposition
        elif self.create_write_disposition_task_id_key_list is not None and \
                self.create_disposition is None:

            task_id = self.create_write_disposition_task_id_key_list[0]
            key = self.create_write_disposition_task_id_key_list[1]

            print("task_id: ", task_id)
            print("key :", key)

            _create_disposition = task_instance.xcom_pull(task_ids=task_id, key=key)["create_disposition"]
        else:
            self.log.error("[OPTION_WRITE] Parametros ingresados de forma invalida !")
            _create_disposition = None

        return _create_disposition

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)

        if not self.schema_fields:
            if self.schema_object and self.source_format != 'DATASTORE_BACKUP':
                gcs_hook = GoogleCloudStorageHook(
                    google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                    delegate_to=self.delegate_to)
                schema_fields = json.loads(gcs_hook.download(
                    self.bucket,
                    self.schema_object).decode("utf-8"))
            elif self.schema_object is None and self.autodetect is False:
                raise ValueError('At least one of `schema_fields`, `schema_object`, '
                                 'or `autodetect` must be passed.')
            else:
                schema_fields = None

        else:
            schema_fields = self.schema_fields

        # source object options
        print("self.source_objects_task_id:", self.source_objects_task_id)
        print("self.source_objects_key:", self.source_objects_key)

        if self.source_objects is None and (
                self.source_objects_task_id is not None and self.source_objects_key is not None):
            source_uris = [
                'gs://{}/{}'.format(self.bucket, source_object)
                for source_object in context['ti'].xcom_pull(
                    task_ids=self.source_objects_task_id, key=self.source_objects_key)]
        elif self.source_objects is not None and (self.source_objects_task_id is None or self.source_objects_key is None):
            source_uris = ['gs://{}/{}'.format(self.bucket, source_object)
                           for source_object in self.source_objects]
        else:
            self.log.error("Parametros ingresados de forma invalida !")
            return None

        self.create_disposition = self._get_create_disposition(context["ti"])
        self.write_disposition = self._get_write_disposition(context["ti"])

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        if self.external_table:
            cursor.create_external_table(
                external_project_dataset_table=self.destination_project_dataset_table,
                schema_fields=schema_fields,
                source_uris=source_uris,
                source_format=self.source_format,
                compression=self.compression,
                skip_leading_rows=self.skip_leading_rows,
                field_delimiter=self.field_delimiter,
                max_bad_records=self.max_bad_records,
                quote_character=self.quote_character,
                ignore_unknown_values=self.ignore_unknown_values,
                allow_quoted_newlines=self.allow_quoted_newlines,
                allow_jagged_rows=self.allow_jagged_rows,
                encoding=self.encoding,
                src_fmt_configs=self.src_fmt_configs,
                encryption_configuration=self.encryption_configuration
            )
        else:
            cursor.run_load(
                destination_project_dataset_table=self.destination_project_dataset_table,
                schema_fields=schema_fields,
                source_uris=source_uris,
                source_format=self.source_format,
                autodetect=self.autodetect,
                create_disposition=self.create_disposition,
                skip_leading_rows=self.skip_leading_rows,
                write_disposition=self.write_disposition,
                field_delimiter=self.field_delimiter,
                max_bad_records=self.max_bad_records,
                quote_character=self.quote_character,
                ignore_unknown_values=self.ignore_unknown_values,
                allow_quoted_newlines=self.allow_quoted_newlines,
                allow_jagged_rows=self.allow_jagged_rows,
                encoding=self.encoding,
                schema_update_options=self.schema_update_options,
                src_fmt_configs=self.src_fmt_configs,
                time_partitioning=self.time_partitioning,
                cluster_fields=self.cluster_fields,
                encryption_configuration=self.encryption_configuration)

        if cursor.use_legacy_sql:
            escaped_table_name = '[{}]'.format(self.destination_project_dataset_table)
        else:
            escaped_table_name = '`{}`'.format(self.destination_project_dataset_table)

        if self.max_id_key:
            cursor.execute('SELECT MAX({}) FROM {}'.format(
                self.max_id_key,
                escaped_table_name))
            row = cursor.fetchone()
            max_id = row[0] if row[0] else 0
            self.log.info(
                'Loaded BQ data with max %s.%s=%s',
                self.destination_project_dataset_table, self.max_id_key, max_id
            )
            return max_id


class FCastGenerateQueryOperator(BaseOperator):
    """Generate sql query parsed from params

    Args:
        :param bigquery_conn_id: Reference to a specific bigquery hook.
        :type bigquery_conn_id: str
        :param destination_dateset_table: GCP Bigquery table {dataset}.{table_name}.
        :type destination_dateset_table: str
        :param path_to_extract: File path to save table's param ("table", "columns",
                                "name_view", "dependency").
        :type path_to_extract: str
        :param table_prefix: Table prefix. This is to help find table name.
        :type table_prefix: str
        :param sql_change_path: Path where the generated sql query will be saved.
        :type sql_change_path: str
        :param file_names_csv: Csv file with columns and tables names and columns types.
        :type file_names_csv: str
        :param generate_view: If `True` sql query will be generated as view. (Default: `False`)
        :type generate_view: bool
        :param dataset_prod: Gcp bigquery dataset where sql results will be saved.
        :type dataset_prod: str
    """
    @apply_defaults
    def __init__(self,
                 bigquery_conn_id: str,
                 destination_dateset_table: str,
                 path_to_extract: str,
                 table_prefix: str,
                 sql_change_path: str,
                 file_names_csv: str,
                 source_table: str = None,
                 generate_backend_info: bool = True,
                 generate_view: bool = False,
                 query_operation: str = "change_name",
                 dataset_prod: str = "tb_sgl_tlm",
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        dataset, table = destination_dateset_table.split(".")
        self.dataset_table = destination_dateset_table
        self.dataset = dataset
        self.table = table
        self.table_prefix = table_prefix
        self.path_to_extract = path_to_extract
        self.sql_change_path = sql_change_path
        self.file_names_csv = file_names_csv
        self.generate_view = generate_view
        self.dataset_prod = dataset_prod
        self.query_operation = query_operation
        self.generate_backend_info = generate_backend_info
        self.source_table = source_table

    def _generate_extract_from_bq(self):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)
        sql_column_info = \
            get_query_get_column_info_bq(dataset_id=self.dataset,
                                         table_name_bq=self.table)
        _bq_client = bigquery.Client(project=bq_hook._get_field("project"),
                                     credentials=bq_hook._get_credentials())
        df_info = _bq_client.query(sql_column_info).to_dataframe()

        list_column = df_info.column_name.unique().tolist()

        # key = value
        dict_column = {column: column for column in list_column}

        if self.source_table is None:
            if self.table.startswith(self.table_prefix):
                table_aux = self.table[len(self.table_prefix):]
            else:
                table_aux = self.table
        else:
            table_aux = self.source_table

        dict_extract = dict()
        df_file_names = pd.read_csv(self.file_names_csv)
        name_view = \
            df_file_names[df_file_names["tabla_nick"].str.upper() == table_aux.upper()]["NombreBQ_nick"]

        if name_view.shape[0] == 0:
            print("[WARNING] Nick tabla {} no existe en archivo {}".format(table_aux, self.file_names_csv))
            name_view = table_aux
        else:
            name_view = name_view.iloc[0]
        # table name uppercase and lowercase
        dict_extract["table"] = table_aux
        dict_extract["columns"] = dict_column
        dict_extract["name_view"] = name_view
        dict_extract["dependency"] = dict()

        path_to_create = self.path_to_extract.replace(self.path_to_extract.split("/")[-1], "")
        createDirectory(path_to_create)

        with open(self.path_to_extract, 'w') as fp:
            json.dump(dict_extract, fp, indent=4)

    def _get_partition_convertion(self):
        # copy table
        query_base = f'CREATE OR REPLACE TABLE {self.dataset_table}_copy AS \n'
        query_selected_copy = f'SELECT * FROM {self.dataset_table} ;\n'
        query_base += query_selected_copy

        # drop table
        query_drop = f'DROP TABLE IF EXISTS {self.dataset_table}; \n'
        query_base += query_drop

        # create table partitioned from copy table
        query_base += f'CREATE OR REPLACE TABLE {self.dataset_table} \n'
        config_extract = json.load(open(self.path_to_extract))
        incremental_column = get_nick(config_extract["sort_by"].replace("\"", ""))
        query_partition_by = f'PARTITION BY {incremental_column} AS \n'
        query_base += query_partition_by
        query_query_from = f'SELECT * \n\
        FROM (SELECT * EXCEPT ({incremental_column}),\n\
        CASE WHEN {incremental_column} = \"00000000\" THEN SAFE.PARSE_DATE(\"%Y%m%d\", \"19000101\")\n\
            ELSE SAFE.PARSE_DATE("%Y%m%d", {incremental_column}) END AS {incremental_column}\n\
        FROM {self.dataset_table}_copy)\n\
        WHERE {incremental_column} > DATE_SUB(CURRENT_DATE(), INTERVAL 4000 DAY);\n'
        query_base += query_query_from

        # drop copy table
        query_drop_copy = f'DROP TABLE IF EXISTS {self.dataset_table}_copy; \n'
        query_base += query_drop_copy

        path_to_create = self.sql_change_path.replace(self.sql_change_path.split("/")[-1], "")
        createDirectory(path_to_create)

        with open(self.sql_change_path, 'w', newline='') as f_handle:
            f_handle.write(query_base)

        return None

    def _get_delete_partition(self):
        config_extract = json.load(open(self.path_to_extract))
        incremental_column = get_nick(config_extract["sort_by"].replace("\"", ""))
        query_base = f'DELETE {self.dataset_table}\n'
        where_query = f'WHERE {incremental_column} < \
            DATE_SUB((SELECT MAX({incremental_column}) FROM {self.dataset_table}), INTERVAL 3950 DAY);\n'
        query_base += where_query

        path_to_create = self.sql_change_path.replace(self.sql_change_path.split("/")[-1], "")
        createDirectory(path_to_create)

        with open(self.sql_change_path, 'w', newline='') as f_handle:
            f_handle.write(query_base)

        return None

    def execute(self, context):
        if self.generate_backend_info:
            self._generate_extract_from_bq()
        else:
            if os.path.isfile(self.path_to_extract):
                print("Found {}".format(self.path_to_extract))
            else:
                print("This file {} needs to be created ".format(self.path_to_extract))
                pass

        if self.query_operation.upper() == "CHANGE_NAME":
            get_query_change_column(sql_change_path=self.sql_change_path,
                                    path_to_extract=self.path_to_extract,
                                    file_names_csv=self.file_names_csv,
                                    table_bq=self.dataset_table,
                                    view=self.generate_view,
                                    dataset_prod=self.dataset_prod)

        elif self.query_operation.upper() == "CONVERT_PARTITION":
            self._get_partition_convertion()
        elif self.query_operation.upper() == "DELETE_PARTITION":
            self._get_delete_partition()
        else:
            print("Selected query operation {} is invalid".format(self.query_operation))
