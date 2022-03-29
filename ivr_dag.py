from includes.operators_update import FCastFilePickUpdaterOperator, FCastCustomGCSToBQOperator
from includes.operators_update import FCastGenerateQueryOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.models import Variable

email_alert_config = Variable.get("email_parameters",
                                  deserialize_json=True)
default_args = {
    'owner': '4Cast',
    'depends_on_past': False,
    'retries': 3,
    'email': email_alert_config["emails"],
    'email_on_failure': email_alert_config["email_on_failure"],
    'start_date': days_ago(1),
}

# Appends the last file to BQ
dag = DAG(
    'ivr_updater',
    default_args=default_args,
    schedule_interval="0 6 * * *"
)

export_config = Variable.get(
    "gcs_to_bq_tb_ivr",
    deserialize_json=True
)

pick_folder_task = FCastFilePickUpdaterOperator(
    task_id="pick_file",
    pick_folder=export_config["pick_folder"],
    google_cloud_storage_conn_id=export_config["google_cloud_storage_conn_id"],
    bucket=export_config["bucket"],
    prefix_blob=export_config["prefix_blob"],
    suffix=export_config["suffix"],
    date_format_increment=export_config["date_format_increment"],
    dag=dag)

gcs_bq = FCastCustomGCSToBQOperator(
    task_id="load_to_bq",
    bucket=export_config["bucket"],
    source_objects_task_id_key_list=["pick_file", "folders"],
    destination_project_dataset_table=export_config["destination_project_dataset_table"],
    schema_object=export_config["schema_object"],
    skip_leading_rows=export_config["skip_leading_rows"],
    source_format=export_config["source_format"],
    create_write_disposition_task_id_key_list=["pick_file", "config_disposition"],
    google_cloud_storage_conn_id=export_config["google_cloud_storage_conn_id"],
    bigquery_conn_id=export_config["bigquery_conn_id"],
    field_delimiter=export_config["field_delimiter"],
    max_bad_records=export_config["max_bad_records"],
    dag=dag)

bq_job = BigQueryExecuteQueryOperator(
    task_id="bq_distinc",
    sql=export_config["sql_job"],
    gcp_conn_id=export_config["bigquery_conn_id"],
    use_legacy_sql=export_config["use_legacy_sql"],
    destination_dataset_table=export_config["destination_project_dataset_table"],
    create_disposition=export_config["create_disposition"],
    write_disposition=export_config["write_disposition"],
    dag=dag
)

bq_schema = BigQueryExecuteQueryOperator(
    task_id="bq_update_schema",
    sql="querys/ivr_schema/ivr.sql",
    gcp_conn_id=export_config["bigquery_conn_id"],
    use_legacy_sql=export_config["use_legacy_sql"],
    create_disposition=export_config["create_disposition"],
    write_disposition=export_config["write_disposition"],
    dag=dag
)

generate_change_name_query_full = FCastGenerateQueryOperator(
    task_id="generate_query_change_full",
    bigquery_conn_id=export_config["bigquery_conn_id"],
    destination_dateset_table=export_config["destination_project_dataset_table"],
    path_to_extract=export_config["path_to_extract"],
    table_prefix="tbl_",
    source_table=export_config["source_name"],
    sql_change_path=export_config["sql_change_path"],
    file_names_csv=export_config["file_names_csv"],
    generate_view=export_config["generate_view"],
    dataset_prod=export_config["dataset_prod"],
    dag=dag
)
bq_job_change_full = BigQueryExecuteQueryOperator(
    task_id="bq_change_columns_full",
    sql=export_config["sql_change_path"].replace("dags/", ""),
    gcp_conn_id=export_config["bigquery_conn_id"],
    use_legacy_sql=export_config["use_legacy_sql"],
    dag=dag
)

pick_folder_task >> gcs_bq >> bq_job >> bq_schema >> generate_change_name_query_full >> bq_job_change_full
