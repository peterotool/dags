# import pyhdb
import csv
import json
import os
import shutil
import pandas as pd

from typing import List, Dict, Union

from .getDataAndSchedule import setup_logging
from . import uploadToStorage as uploader
from datetime import date, datetime, timedelta


import logging
setup_logging()
logger = logging.getLogger(__name__)


def getConfigBW(path: str = "conf/operation/configBW.json") -> Dict:
    """[Load data config]

    Args:
        path (str, optional): [Path to read from]. Defaults to "conf/operation/configBW.json".

    Returns:
        Dict: [description]
    """
    with open(path) as config_file:
        data = json.load(config_file)
    return data


def getConnection(path_config_BW: str = "conf/operation/configBW.json"):
    """[Get connection with pyhdb ]

    Args:
        path_config_BW (str, optional): [Path to config file]. Defaults to "conf/operation/configBW.json".

    Returns:
        [pyhdb.connect]: [pyhdb connection]
    """

    dataConfig = getConfigBW(path_config_BW)
    connection = pyhdb.connect(
        host=dataConfig["host"],
        port=dataConfig["port"],
        user=dataConfig["user"],
        password=dataConfig["password"]
    )

    return connection


def writeQuery(path_to_local: str,
               query_string: str,
               list_columns: List,
               path_to_storage: str,
               credential_path: str,
               path_config_bw: str,
               small_test: bool = False,
               date_today: str = None,
               limit_x: int = 20000,
               upload_option: bool = True) -> None:
    """[Query to server and download data]

    Args:
        date_today (str): [-]
        path_to_local (str): [tmp path to download]
        query_string (str): [-]
        list_columns (List): [columnos in csv file]
        path_to_storage (str): [-]
        credential_path (str): [credentials to upload data]
        path_config_bw (str): [path config bw]
        small_test (bool, optional): [for small testing]. Defaults to False.
        limit_x (int, optional): [limit to download each time]. Defaults to 20000.
        upload_option (bool, optional): [upload or not]. Defaults to True.

    Returns:
        [None]: [None]
    """

    if date_today is not None:
        pass
    else:
        print("Upload option is active, but date_today is None")
        date_today = "daily_update"

    total_query = "SELECT COUNT(*) FROM ({})".format(query_string)

    connection = getConnection(path_config_bw)
    cursor = connection.cursor()

    logger.info("Total Query: " + str(total_query))
    cursor.execute(total_query)

    result_total = cursor.fetchall()
    total_transactions = result_total[0][0]
    logger.info("Total rows: " + str(total_transactions))

    range_for = int(total_transactions/limit_x + 1)

    cursor.execute(query_string)

    for x in range(0, range_for):

        offset_aux = limit_x*x
        result = cursor.fetchmany(limit_x)
        len_result = len(result)

        if len_result > 0:
            dir_aux = path_to_local + date_today + "/" + str(offset_aux) + ".csv"

            saveCSVFromDB(date_today=date_today,
                          header=list_columns,
                          result=result,
                          path_to_save=dir_aux,
                          path_to_storage=path_to_storage,
                          credential_path=credential_path,
                          upload=upload_option)

            logger.info("For id: " + str(x + 1) + "/"
                        + str(range_for) + " Number of transactions: " + str(len_result))
        else:
            logger.info("No results for id: " + str(x + 1) + "/"
                        + str(range_for) + " Number of transactions: " + str(len_result))

        if small_test:  # Quizas pasar a rama dev
            break
        else:
            pass

    logger.info("Querying data ended")

    connection.close()

    return None


def saveCSVFromDB(header: List,
                  result,
                  path_to_save: str,
                  path_to_storage: str = "",
                  upload: bool = False,
                  credential_path: str = "conf/json_key.json",
                  date_today: str = None) -> None:
    """[Save csv file and upload/remove when done]

    Args:
        header (List): [csv header]
        result ([type]): [result to save as csv]
        path_to_save (str): [-]
        path_to_storage (str, optional): [-]. Defaults to "".
        upload (bool, optional): [upload or not]. Defaults to False.
        credential_path (str, optional): [credential for uploading]. Defaults to "conf/json_key.json".

    Returns:
        [None]: [None]
    """

    with open(path_to_save, 'w', newline='') as f_handle:

        writer = csv.writer(f_handle)
        writer.writerow(header)

        for row in result:
            writer.writerow(row)

    path_to_local, file_name = os.path.split(os.path.abspath(path_to_save))

    if upload:
        if date_today is not None:
            pass
        else:
            print("Upload option is active, but date_today is None")
            date_today = "daily_update"

        bucket_name = path_to_storage.split("/")[0]
        source_file_name = path_to_local + "/" + file_name
        destination_blob_name = path_to_storage.replace(bucket_name + "/", "") + date_today + "/" + file_name

        uploader.upload_to_storage(bucket_name,
                                   source_file_name,
                                   destination_blob_name,
                                   credential_path)

        os.remove(path_to_save)
    else:
        pass

    return None


def make_select_base(dict_table: Dict) -> Union[str, List]:
    """[Make base select from dict]

    Args:
        dict_table (Dict): [dict with columns in table]

    Returns:
        Union[str, List]: [query string for db, list with columns in table]
    """

    list_columns_with_nick = ""
    base_schema = " \"SAPABAP1\"."
    aux_table = "T_TABLE"
    column_list = list()
    logger.info(sorted(dict_table["columns"]))
    dict_columns = {key: value for key, value in sorted(dict_table["columns"].items())}

    for column_nick in dict_columns.keys():
        list_columns_with_nick += aux_table + ".\"" \
            + dict_columns[column_nick] \
            + "\" AS " + column_nick + ", "

        column_list.append(column_nick)

    list_columns_with_nick = list_columns_with_nick[:-2]

    SQL_QUERY = f'\n\
                SELECT {list_columns_with_nick} \n\
                FROM {base_schema}{dict_table["table"]} AS {aux_table} '

    return SQL_QUERY, column_list


def make_select_full(dict_table: Dict,
                     is_big_table: bool = False) -> Union[str, List]:
    """[Make full select from dict]

    Args:
        dict_table (Dict): [dict with columns in table]

    Returns:
        Union[str, List]: [query string for db, list with columns in table]
    """
    sql_query, column_list = make_select_base(dict_table)

    if dict_table["sort_by"] == '\"\"':
        pass
    else:
        orderby_query = f'\n\
                        ORDER BY {dict_table["sort_by"]} ASC \n\
                        '
        where_query = f'\n\
            WHERE {dict_table["sort_by"]} >= \'20100101\' \n\
                '
        if is_big_table:
            sql_query += where_query
        else:
            pass

        sql_query += orderby_query

    return sql_query, column_list


def make_select_incremental_date(dict_table: Dict) -> Union[str, List]:
    """[Make full select from dict]

    Args:
        dict_table (Dict): [dict with columns in table]

    Returns:
        Union[str, List]: [query string for db, list with columns in table]
    """
    aux_table = "T_TABLE"
    SQL_QUERY, column_list = make_select_base(dict_table)
    SQL_QUERY = SQL_QUERY + f'\n\
                WHERE {aux_table}.{dict_table["sort_by"]} >= {get_date_days_ago(dict_table["delta_date_from"])}  \n\
                ORDER BY {dict_table["sort_by"]} ASC \n'

    return SQL_QUERY, column_list


def make_select(dict_table: Dict) -> Union[str, List]:
    """[Make select from dict]

    Args:
        dict_table (Dict): [dict with columns in table]

    Returns:
        Union[str, List]: [query string for db, list with columns in table]
    """
    return make_select_full(dict_table)


def getDataForFileAndQuery(path_to_check: str,
                           credential_path: str,
                           path_config_bw: str,
                           small_test: bool,
                           size_transactions: int,
                           upload: bool,
                           date_today: str = None,
                           is_big_table: bool = False,
                           incremental_download: bool = False,
                           version_data: str = "V1") -> None:
    """[Read json and download data from DB and upload to storage]

    Args:
        path_to_check (str): [json file with description of table]
        date_today (str): [-]
        credential_path (str): [credential for uploading]
        path_config_bw (str): [path to bw credentials]
        small_test (bool): [for small testing]
        size_transactions (int): [number of rows to download]
        upload (bool): [upload or not]

    Returns:
        [None]: [None]
    """

    if os.path.isfile(path_to_check):
        pass
    else:
        logger.error("Archivo : '{}' no existe".format(path_to_check))
        return None

    with open(path_to_check, 'r') as f:
        dict_table = json.load(f)

    path_to_local = os.path.join(os.getcwd(), "tmp", dict_table["path_to_local"])

    createDirectory(path_to_local)

    path_to_storage = dict_table["path_to_storage"] + version_data + "/"

    if incremental_download == True:
        query_string, list_columns = make_select_incremental_date(dict_table)
    else:  # full download
        query_string, list_columns = make_select_full(dict_table,
                                                      is_big_table=is_big_table)

    if date_today is not None:
        date_today = date_today[0:4] + "/" + date_today[4:6] + "/" + date_today[6:8]
    else:
        print("Upload option is active, but date_today is None")
        date_today = "daily_update"

    today_directory = path_to_local + date_today

    createDirectory(today_directory)

    writeQuery(date_today=date_today,
               path_to_local=path_to_local,
               query_string=query_string,
               list_columns=list_columns,
               path_to_storage=path_to_storage,
               small_test=small_test,
               credential_path=credential_path,
               path_config_bw=path_config_bw,
               limit_x=size_transactions,
               upload_option=upload)

    deleteDirectory(today_directory)

    return None


def createDirectory(today_directory: str) -> None:
    """[Create directory with today date]

    Args:
        today_directory (str): [-]

    Returns:
        [None]: [None]
    """
    try:
        os.makedirs(today_directory, exist_ok=True)
        logger.info("Directory '%s' created successfully" % today_directory)
    except OSError as error:
        logger.info("Directory '{}' can not be created Error: '{}'".format(today_directory, error))
    return None


def deleteDirectory(directory_to_remove: str) -> None:
    shutil.rmtree(directory_to_remove)
    logger.info("Directory '%s' removed successfully" % directory_to_remove)
    pass


def get_nick(base_name: str) -> str:
    """[Get nick when / appears in column name]

    Args:
        base_name (str): [original name]

    Returns:
        str: [adapted name]
    """

    nickname = base_name
    nickname = nickname[:3].replace('/', '') + nickname[3:]
    nickname = nickname.replace('\"', '')
    nickname = nickname.replace('/', '_')

    return nickname


def make_extraction_files(path_to_check: str = "conf/base_conf/bw_tables/definitions/",
                          path_to_extract: str = "conf/base_conf/bw_tables/extraction/") -> None:
    """[Make base json files configuration for extraction]

    Args:
        path_to_check (str, optional): [path with csv description of tables]. 
                                       Defaults to "conf/base_conf/bw_tables/definitions/".
        path_to_extract (str, optional): [path to write the extraction json file]. 
                                         Defaults to "conf/base_conf/bw_tables/extraction/".

    Returns:
        [None]: [None]
    """

    files_in_path = [file for file in os.listdir(path_to_check)
                     if (os.path.isfile(os.path.join(path_to_check, file)) and file.endswith('.csv'))]

    for file_to_check in files_in_path:
        df_definition = pd.read_csv(path_to_check+file_to_check)

        extraction_dict = dict()
        extraction_dict["table"] = "\"{}\"".format(df_definition['TABLE_NAME'].iloc[0])
        table_nick = get_nick(extraction_dict["table"])
        extraction_dict["path_to_local"] = "data/DS67/{}/v1/".format(table_nick)
        extraction_dict["path_to_storage"] = "edms-dev-tests/lip-landing/BW/{}/".format(table_nick)
        extraction_dict["sort_by"] = ""
        extraction_dict["columns"] = dict()

        for index, row in df_definition.iterrows():
            extraction_dict["columns"][get_nick(row['COLUMN_NAME'])] = row['COLUMN_NAME']

        file_path = path_to_extract + "{}.json".format(table_nick)
        with open(file_path, 'w') as fp:
            json.dump(extraction_dict, fp, indent=4)

        pass

    return None


def get_dict_dependency(df_table):
    DEPENDENCY_NICK = "dependencia_nick"
    COLUMN_NICK = "Column_nick"
    dict_out = dict()
    # unique dependency
    for column, dependency_column in zip(df_table[COLUMN_NICK], df_table[DEPENDENCY_NICK]):
        if pd.isnull(dependency_column):
            pass
        else:
            dict_out[dependency_column] = column

    return dict_out


def get_extraction(blob_storage_base: str,
                   path_to_write: str, path_to_extract: str,
                   incremental_column: str = "",
                   delta_incremental: str = "30",
                   path_file_names_csv: str = None) -> None:

    df_definition = pd.read_csv(path_to_write)
    dict_extraction = dict()

    name_table_bw = df_definition['TABLE_NAME'].iloc[0]
    dict_extraction["table"] = "\"{}\"".format(name_table_bw)

    table_nick = get_nick(dict_extraction["table"])

    dict_extraction["path_to_local"] = "data/DS67/{}/".format(table_nick)
    dict_extraction["path_to_storage"] = blob_storage_base
    dict_extraction["sort_by"] = "\"{}\"".format(incremental_column)
    dict_extraction["columns"] = dict()
    dict_extraction["delta_date_from"] = delta_incremental

    if path_file_names_csv is not None:
        df_file_names = pd.read_csv(path_file_names_csv)
        df_file_names = df_file_names[df_file_names["tabla_nick"] == table_nick]
        name_view = df_file_names["NombreBQ_nick"]

        if name_view.shape[0] == 0:
            print("[WARNING] Nick tabla {} no existe en archivo {}".format(table_nick, path_file_names_csv))
            name_view = table_nick
            dict_dependency = dict()
        else:
            name_view = name_view.iloc[0]
            dict_dependency = get_dict_dependency(df_file_names)

        dict_extraction["name_view"] = name_view
        dict_extraction["dependency"] = dict_dependency

    for index, row in df_definition.iterrows():
        dict_extraction["columns"][get_nick(row['COLUMN_NAME'])] = row['COLUMN_NAME']

    path_to_create = path_to_extract.replace(path_to_extract.split("/")[-1], "")
    createDirectory(path_to_create)

    with open(path_to_extract, 'w') as fp:
        json.dump(dict_extraction, fp, indent=4)

    return None


def get_definitions(path_to_check: str = "conf/base_conf/bw_tables/conf.json",
                    path_to_write: str = "conf/base_conf/bw_tables/definitions/",
                    path_config_bw: str = "conf/operation/configBW.json") -> None:
    """[Get definitions/schemas of tables]

    Args:
        path_to_check (str, optional): [file with list of tables to check]. 
                                       Defaults to "conf/base_conf/bw_tables/conf.json".
        path_to_write (str, optional): [path to write definitions]. 
                                       Defaults to "conf/base_conf/bw_tables/definitions/".
        path_config_bw (str, optional): [path with BW config]. 
                                        Defaults to "conf/operation/configBW.json".

    Returns:
        [None]: [None]
    """

    if os.path.isfile(path_to_check):
        pass
    else:
        logger.error("Config file doesnt exists {}".format(path_to_check))
        return None

    with open(path_to_check) as config_file:
        conf_tables = json.load(config_file)

    list_column = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE_NAME", "LENGTH"]
    base_query = "SELECT  "

    for column in list_column:
        base_query = base_query + column + ", "

    base_query = base_query[:-2] \
        + " FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME='SAPABAP1' AND TABLE_NAME="

    connection = getConnection(path_config_bw)
    cursor = connection.cursor()

    createDirectory(path_to_write)

    for table in conf_tables["tables"]:

        query_string = base_query + table
        logger.info("Query: "+str(query_string))

        cursor.execute(query_string)
        result = cursor.fetchall()
        len_result = len(result)

        if len_result > 0:

            table_nick = get_nick(result[0][0])
            dir_aux = path_to_write+str(table_nick)+'.csv'

            saveCSVFromDB(header=list_column,
                          result=result,
                          path_to_save=dir_aux)

            logger.info("For id: " + str(table_nick) + " definitions are saved")
        else:
            pass

        pass

    connection.close()

    return None


def get_definition(name_table: str,
                   path_to_write: str,
                   path_config_bw: str) -> None:

    list_column = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE_NAME", "LENGTH"]
    base_query = "SELECT  "

    for column in list_column:
        base_query = base_query + column + ", "

    base_query = base_query[:-2] \
        + " FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME='SAPABAP1' AND TABLE_NAME="

    base_query += "'{}'".format(name_table)
    connection = getConnection(path_config_bw)
    cursor = connection.cursor()

    table_nick = get_nick(name_table)
    path_to_create = path_to_write.replace(path_to_write.split("/")[-1], "")

    print(os.getcwd() + "/" + path_to_create)
    createDirectory(path_to_create)

    print("Query schema: "+str(base_query))

    cursor.execute(base_query)

    result = cursor.fetchall()
    len_result = len(result)

    if len_result > 0:

        table_nick = get_nick(result[0][0])
        dir_aux = os.getcwd() + "/" + path_to_write

        saveCSVFromDB(header=list_column,
                      result=result,
                      path_to_save=dir_aux)

        print("For table: " + str(table_nick) + " definitions are saved")
    else:
        pass

    connection.close()

    return None


def get_query_merge_job(sql_job_path: str,
                        path_to_extract: str,
                        table_tbl: str,
                        table_wrk: str,
                        table_name: str,
                        path_config_bw: str) -> None:

    config_extract = json.load(open(path_to_extract))

    dict_columns = {key: value for key, value in sorted(config_extract["columns"].items())}
    list_columns = list(dict_columns.keys())

    list_id_columns = get_id_columns(table_name, path_config_bw)

    # MERGE .. USING ..
    merge_query_base = \
        f'MERGE \n\
        {table_tbl} AS t1 \n\
    USING \n\
        {table_wrk} AS t2 \n'
    on_merge_query = f'\nON \n'

    # ON
    for i, column_id in enumerate(list_id_columns):
        if len(list_id_columns) == i + 1:
            on_merge_query += f' t1.{column_id} = t2.{column_id} \n'
        else:
            on_merge_query += f' t1.{column_id} = t2.{column_id} AND \n'

    merge_query_base += on_merge_query

    # WHEN MATCHED
    when_matched_query = \
        f'WHEN MATCHED \n\
	 THEN \n\
	 UPDATE \n\
	 SET \n'

    for i, column in enumerate(list_columns):
        if len(list_columns) == i + 1:
            when_matched_query += f' t1.{column} = t2.{column} \n'
        else:
            when_matched_query += f' t1.{column} = t2.{column} ,\n'

    merge_query_base += when_matched_query

    # WHEN NOT MATCHED
    when_not_matched_query = \
        f'WHEN NOT MATCHED THEN \n\
     INSERT( \n\
    '

    for i, column in enumerate(list_columns):
        if len(list_columns) == i + 1:
            when_not_matched_query += f' {column} ) \n'
        else:
            when_not_matched_query += f' {column} , \n'

    when_not_matched_query += f'VALUES( \n'

    for i, column in enumerate(list_columns):
        if len(list_columns) == i + 1:
            when_not_matched_query += f' t2.{column} ) \n'
        else:
            when_not_matched_query += f' t2.{column} , \n'

    merge_query_base += when_not_matched_query

    path_create = sql_job_path.replace(sql_job_path.split("/")[-1], "")
    createDirectory(path_create)

    with open(sql_job_path, 'w', newline='') as f_handle:
        f_handle.write(merge_query_base)

    return None


def get_query_get_column_info_bq(dataset_id: str,
                                 table_name_bq: str,
                                 project_id: str = None,
                                 column_info: str = "*"):

    base_query = f'SELECT {column_info}\n'
    if project_id is not None:
        from_query = f'FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`\n'
    else:
        from_query = f'FROM `{dataset_id}.INFORMATION_SCHEMA.COLUMNS`\n'
    base_query += from_query

    where_table_query = f'WHERE table_name = \"{table_name_bq}\"'
    base_query += where_table_query

    return base_query


def get_operation_time_dependency(column_dependency: str,
                                  column: str):
    operation_out = \
        f'CASE \n\
        WHEN {column_dependency} = \"00000000\" THEN SAFE.PARSE_TIMESTAMP(\"%Y%m%d%H%M%S\", CONCAT(\"19000101\", {column}))\n\
        ELSE SAFE.PARSE_TIMESTAMP(\"%Y%m%d%H%M%S\", CONCAT({column_dependency},{column})) END'
    return operation_out


def get_query_change_column(sql_change_path: str,
                            path_to_extract: str,
                            file_names_csv: str,
                            table_bq: str,
                            view: bool = False,
                            dataset_prod: str = "tb_bw") -> None:

    COLUMN_NICK = "Column_nick"
    TABLE_NICK = "tabla_nick"
    COLUMN_NICK_BQ = "Column_nick_bq"

    config_extract = json.load(open(path_to_extract))

    dict_columns = {key: value for key, value in sorted(config_extract["columns"].items())}
    list_columns = list(dict_columns.keys())

    table_name = get_nick(config_extract["table"].replace("\"", ""))
    dict_dependency = config_extract["dependency"]

    df_names = pd.read_csv(file_names_csv)

    if view:
        name_view = config_extract["name_view"]
        base_query = f'CREATE OR REPLACE VIEW \n'
        base_query += f'{dataset_prod}.{name_view} AS \n'
    else:
        base_query = f'\n'
    select_query = f'SELECT \n'

    # name AS and time dependency
    for column in list_columns:

        column_name = df_names[(df_names[COLUMN_NICK] == column)
                               & ((df_names[TABLE_NICK] == table_name)
                               | (df_names[TABLE_NICK] == table_name.upper()))]
        if column_name.shape[0] >= 1:
            column_name = column_name[COLUMN_NICK_BQ + "_aux"].iloc[0]
            if dict_dependency:
                if column in dict_dependency.keys():
                    operation_column = get_operation_time_dependency(column, dict_dependency[column])
                    select_query += f'{operation_column} AS {column_name}, \n'
                elif column in dict_dependency.values():
                    pass
                else:
                    operation_column = column
                    select_query += f'{operation_column} AS {column_name}, \n'
            else:
                operation_column = column
                select_query += f'{operation_column} AS {column_name}, \n'
        else:
            print("[WARNING] columna '{}' de la tabla '{}' no tiene traduccion en el archivo '{}'"
                  .format(column, table_name, file_names_csv))
            column_name = column
            operation_column = column
            select_query += f'{operation_column} AS {column_name}, \n'

        #select_query += f'{operation_column} AS {column_name}, \n'

    base_query += select_query
    base_query += f'FROM {table_bq}'
    path_to_create = sql_change_path.replace(sql_change_path.split("/")[-1], "")
    createDirectory(path_to_create)

    with open(sql_change_path, 'w', newline='') as f_handle:
        f_handle.write(base_query)

    return None


def get_id_columns(table_name: str,
                   path_config_bw: str) -> List:

    base_query = f'SELECT DISTINCT T1.COLUMN_NAME \n\
                FROM "SYS"."CONSTRAINTS" AS T1 \n\
                WHERE  \n\
                T1.SCHEMA_NAME = \'SAPABAP1\' AND \n\
                T1.IS_PRIMARY_KEY = \'TRUE\' AND \n\
                T1.TABLE_NAME = \'{table_name}\' '

    connection = getConnection(path_config_bw)
    cursor = connection.cursor()
    cursor.execute(base_query)

    result = cursor.fetchall()

    list_id_columns = [get_nick(column[0]) for column in result]

    connection.close()

    return list_id_columns


def get_info(table_name,
             path_config_bw="config/conf/operation/configBW.json"):

    list_column = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE_NAME", "LENGTH"]
    base_query = "SELECT * "

    base_query = base_query \
        + " FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME='SAPABAP1' AND TABLE_NAME="

    connection = getConnection(path_config_bw)
    cursor = connection.cursor()

    query_string = base_query + table_name

    print(query_string)

    cursor.execute(query_string)
    result = cursor.fetchall()

    for row in result:
        print(row)
    connection.close()


def get_date_days_ago(days_to_subtract=1):

    days_ago = datetime.today() - timedelta(days=int(days_to_subtract))
    days_ago = days_ago.strftime("%Y%m%d")
    return days_ago
