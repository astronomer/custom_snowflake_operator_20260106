import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator

with DAG(
    "connection_testing",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Test Connections"],
):
    connections = ["snowflake_default"]

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for conn_id in connections:

        @task(task_id=f"check_conn_exists_{conn_id}")
        def check_conn_exists():
            try:
                conn = BaseHook.get_connection(conn_id=conn_id)
                logging.info(
                    f"ID: {conn.conn_id}. Host: {conn.host}, Port: {conn.port}, Schema: {conn.schema}, Login: {conn.login}"
                )
            except Exception:
                logging.error(f"Could not find connection: {conn_id}")
                raise

        @task(task_id=f"test_conn_works_{conn_id}")
        def test_conn_works():
            try:
                conn = BaseHook.get_connection(conn_id=conn_id)
                status, message = conn.test_connection()
                logging.info(
                    f"Testing connection {conn_id}. Status: {status}. Message: {message}"
                )
                if not status:
                    raise ConnectionError(
                        f"Connection test failed for {conn_id}. Message: {message}"
                    )
            except Exception:
                logging.error(f"Could not test connection {conn_id}.")
                raise

        start >> check_conn_exists() >> test_conn_works() >> end
