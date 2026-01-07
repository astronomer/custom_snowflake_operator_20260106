from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from include.operators.snowflake import SnowflakeSqlApiOperator

snowflake_conn_id = "snowflake_default"
snowflake_single_query = "SELECT CURRENT_TIMESTAMP();"
snowflake_multi_query = """
SELECT CURRENT_DATABASE();
SELECT CURRENT_WAREHOUSE();
SELECT CURRENT_TIMESTAMP();
"""


with DAG(
    "example_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
):
    # Single query
    sql_single = SQLExecuteQueryOperator(
        task_id="sql_single",
        sql=snowflake_single_query,
        conn_id=snowflake_conn_id,
        do_xcom_push=True,
    )

    api_single = SnowflakeSqlApiOperator(
        task_id="api_single",
        sql=snowflake_single_query,
        conn_id=snowflake_conn_id,
        deferrable=False,
        do_xcom_push=True,
    )

    api_single_defer = SnowflakeSqlApiOperator(
        task_id="api_single_defer",
        sql=snowflake_single_query,
        conn_id=snowflake_conn_id,
        deferrable=True,
        do_xcom_push=True,
    )

    # Multi query
    sql_multi = SQLExecuteQueryOperator(
        task_id="sql_multi",
        sql=snowflake_multi_query,
        conn_id=snowflake_conn_id,
        split_statements=True,
        return_last=True,
        do_xcom_push=True,
    )

    api_multi = SnowflakeSqlApiOperator(
        task_id="api_multi",
        sql=snowflake_multi_query,
        conn_id=snowflake_conn_id,
        statement_count=3,
        deferrable=False,
        return_last=True,
        do_xcom_push=True,
    )

    api_multi_defer = SnowflakeSqlApiOperator(
        task_id="api_multi_defer",
        sql=snowflake_multi_query,
        conn_id=snowflake_conn_id,
        deferrable=True,
        statement_count=3,
        return_last=True,
        do_xcom_push=True,
    )

    (
        sql_single
        >> api_single
        >> api_single_defer
        >> sql_multi
        >> api_multi
        >> api_multi_defer
    )
