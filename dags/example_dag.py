"""
Example DAG for testing custom SnowflakeSqlApiOperator vs SQLExecuteQueryOperator.

This DAG demonstrates all parameter combinations for the custom SnowflakeSqlApiOperator
and compares XCom output structure with SQLExecuteQueryOperator.

Parameter Descriptions
----------------------

conn_id : str
    Connection ID for Snowflake. The custom operator uses `conn_id` instead of
    `snowflake_conn_id` to align with SQLExecuteQueryOperator interface.

fetch_results : bool
    Controls what gets returned by the operator:
    - True: Fetches and returns actual query results (list of tuples, same as SQLExecuteQueryOperator)
    - False: Returns only query ID strings for async tracking (same as base operator)
    Default: False

deferrable : bool
    Controls execution mode:
    - True: Uses Airflow's triggerer for async execution (frees worker slots)
    - False: Synchronous execution (blocks worker until query completes)
    Recommended for queries > 1 minute.
    Default: False

return_last : bool (multi-query only)
    Controls which results are returned when executing multiple statements:
    - True: Returns only the last query result
    - False: Returns a list of all query results
    Default: True

statement_count : int (multi-query only)
    Number of statements in the SQL query. Required when executing multiple
    statements separated by semicolons.

Notes
-----
- The deferrable parameter does not affect return values, only execution mode.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from include.operators.snowflake import SnowflakeSqlApiOperator

snowflake_conn_id = "snowflake_default"
snowflake_single_query = """
SELECT 
    table_catalog,
    table_schema,
    table_name,
    table_type,
    row_count,
    bytes,
    created,
    last_altered
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'INFORMATION_SCHEMA'
LIMIT 10;
"""
snowflake_multi_query = """
SELECT CURRENT_TIMESTAMP() AS query_1_time;

SELECT CURRENT_DATABASE() AS query_2_database;

SELECT CURRENT_WAREHOUSE() AS query_3_warehouse;
"""


with DAG(
    "example_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
):
    with TaskGroup("single_query") as single_query_group:
        # Expected return: List of tuples with query results (10 rows)
        fetch_results_true_deferrable_false = SnowflakeSqlApiOperator(
            task_id="fetch_results_true_deferrable_false",
            sql=snowflake_single_query,
            conn_id=snowflake_conn_id,
            fetch_results=True,
            deferrable=False,
        )

        # Expected return: List of tuples with query results (10 rows)
        fetch_results_true_deferrable_true = SnowflakeSqlApiOperator(
            task_id="fetch_results_true_deferrable_true",
            sql=snowflake_single_query,
            conn_id=snowflake_conn_id,
            fetch_results=True,
            deferrable=True,
        )

        # Expected return: List with single query ID string
        fetch_results_false_deferrable_false = SnowflakeSqlApiOperator(
            task_id="fetch_results_false_deferrable_false",
            sql=snowflake_single_query,
            conn_id=snowflake_conn_id,
            fetch_results=False,
            deferrable=False,
        )

        # Expected return: List with single query ID string
        fetch_results_false_deferrable_true = SnowflakeSqlApiOperator(
            task_id="fetch_results_false_deferrable_true",
            sql=snowflake_single_query,
            conn_id=snowflake_conn_id,
            fetch_results=False,
            deferrable=True,
        )

    with TaskGroup("multi_query") as multi_query_group:
        # All combinations: return_last (True/False) x fetch_results (True/False) x deferrable (True/False)

        # Expected return: List of tuples with last query result (query 3: warehouse name)
        return_last_true_fetch_true_deferrable_false = SnowflakeSqlApiOperator(
            task_id="return_last_true_fetch_true_deferrable_false",
            sql=snowflake_multi_query,
            conn_id=snowflake_conn_id,
            statement_count=3,
            return_last=True,
            fetch_results=True,
            deferrable=False,
        )

        # Expected return: List of tuples with last query result (query 3: warehouse name)
        return_last_true_fetch_true_deferrable_true = SnowflakeSqlApiOperator(
            task_id="return_last_true_fetch_true_deferrable_true",
            sql=snowflake_multi_query,
            conn_id=snowflake_conn_id,
            statement_count=3,
            return_last=True,
            fetch_results=True,
            deferrable=True,
        )

        # Expected return: List with parent query ID string
        return_last_true_fetch_false_deferrable_false = SnowflakeSqlApiOperator(
            task_id="return_last_true_fetch_false_deferrable_false",
            sql=snowflake_multi_query,
            conn_id=snowflake_conn_id,
            statement_count=3,
            return_last=True,
            fetch_results=False,
            deferrable=False,
        )

        # Expected return: List with parent query ID string
        return_last_true_fetch_false_deferrable_true = SnowflakeSqlApiOperator(
            task_id="return_last_true_fetch_false_deferrable_true",
            sql=snowflake_multi_query,
            conn_id=snowflake_conn_id,
            statement_count=3,
            return_last=True,
            fetch_results=False,
            deferrable=True,
        )

        # Expected return: List of 3 result sets (each is list of tuples)
        return_last_false_fetch_true_deferrable_false = SnowflakeSqlApiOperator(
            task_id="return_last_false_fetch_true_deferrable_false",
            sql=snowflake_multi_query,
            conn_id=snowflake_conn_id,
            statement_count=3,
            return_last=False,
            fetch_results=True,
            deferrable=False,
        )

        # Expected return: List of 3 result sets (each is list of tuples)
        return_last_false_fetch_true_deferrable_true = SnowflakeSqlApiOperator(
            task_id="return_last_false_fetch_true_deferrable_true",
            sql=snowflake_multi_query,
            conn_id=snowflake_conn_id,
            statement_count=3,
            return_last=False,
            fetch_results=True,
            deferrable=True,
        )

        # Expected return: List with parent query ID string
        return_last_false_fetch_false_deferrable_false = SnowflakeSqlApiOperator(
            task_id="return_last_false_fetch_false_deferrable_false",
            sql=snowflake_multi_query,
            conn_id=snowflake_conn_id,
            statement_count=3,
            return_last=False,
            fetch_results=False,
            deferrable=False,
        )

        # Expected return: List with parent query ID string
        return_last_false_fetch_false_deferrable_true = SnowflakeSqlApiOperator(
            task_id="return_last_false_fetch_false_deferrable_true",
            sql=snowflake_multi_query,
            conn_id=snowflake_conn_id,
            statement_count=3,
            return_last=False,
            fetch_results=False,
            deferrable=True,
        )

    # SQLExecuteQueryOperator comparison tasks
    with TaskGroup("sql_execute_query") as sql_execute_query_group:
        # Single query - returns list of tuples by default
        # Expected return: List of tuples [(col1, col2, ...), ...]
        sql_single_query = SQLExecuteQueryOperator(
            task_id="sql_single_query",
            conn_id=snowflake_conn_id,
            sql=snowflake_single_query,
        )

        # Single query with return_last=True (default behavior)
        # Expected return: List of tuples for single query
        sql_single_return_last_true = SQLExecuteQueryOperator(
            task_id="sql_single_return_last_true",
            conn_id=snowflake_conn_id,
            sql=snowflake_single_query,
            return_last=True,
        )

        # Multi-query with split_statements=True, return_last=True (default)
        # Expected return: Last query result only (warehouse name)
        sql_multi_return_last_true = SQLExecuteQueryOperator(
            task_id="sql_multi_return_last_true",
            conn_id=snowflake_conn_id,
            sql=snowflake_multi_query,
            split_statements=True,
            return_last=True,
        )

        # Multi-query with split_statements=True, return_last=False
        # Expected return: List of all 3 query results
        sql_multi_return_last_false = SQLExecuteQueryOperator(
            task_id="sql_multi_return_last_false",
            conn_id=snowflake_conn_id,
            sql=snowflake_multi_query,
            split_statements=True,
            return_last=False,
        )

    # Parameterized query examples
    # SQLExecuteQueryOperator: Uses pyformat (%s or %(name)s) - Snowflake Python connector
    # SnowflakeSqlApiOperator: Uses qmark (?) with bindings dict only - % style is NOT supported
    with TaskGroup("parameterized_queries") as parameterized_queries_group:
        # SQL with positional parameters (%s style) - SQLExecuteQueryOperator
        # Expected return: List of tuples filtered by schema
        sql_positional_params = SQLExecuteQueryOperator(
            task_id="sql_positional_params",
            conn_id=snowflake_conn_id,
            sql="SELECT table_name, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = %s LIMIT 5;",
            parameters=["INFORMATION_SCHEMA"],
        )

        # SQL with named parameters (%(name)s style) - SQLExecuteQueryOperator
        # Expected return: List of tuples filtered by schema and type
        sql_named_params = SQLExecuteQueryOperator(
            task_id="sql_named_params",
            conn_id=snowflake_conn_id,
            sql="SELECT table_name, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = %(schema)s AND table_type = %(type)s LIMIT 5;",
            parameters={"schema": "INFORMATION_SCHEMA", "type": "VIEW"},
        )

        # SQL with positional parameters (? style) - SnowflakeSqlApiOperator
        # Note: SQL API uses ? placeholders with typed bindings dict
        # Expected return: List of tuples filtered by schema
        api_positional_params = SnowflakeSqlApiOperator(
            task_id="api_positional_params",
            conn_id=snowflake_conn_id,
            sql="SELECT table_name, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? LIMIT 5;",
            bindings={"1": {"type": "TEXT", "value": "INFORMATION_SCHEMA"}},
            fetch_results=True,
        )

        # SQL with multiple positional parameters (? style) - SnowflakeSqlApiOperator
        # Expected return: List of tuples filtered by schema and type
        api_multi_params = SnowflakeSqlApiOperator(
            task_id="api_multi_params",
            conn_id=snowflake_conn_id,
            sql="SELECT table_name, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_type = ? LIMIT 5;",
            bindings={
                "1": {"type": "TEXT", "value": "INFORMATION_SCHEMA"},
                "2": {"type": "TEXT", "value": "VIEW"},
            },
            fetch_results=True,
        )

        # Jinja templating - SQLExecuteQueryOperator
        # Uses {{ params.name }} syntax, values are injected at render time (not escaped)
        # Expected return: List of tuples filtered by schema
        sql_jinja_params = SQLExecuteQueryOperator(
            task_id="sql_jinja_params",
            conn_id=snowflake_conn_id,
            sql="SELECT table_name, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{{ params.schema }}' LIMIT 5;",
            params={"schema": "INFORMATION_SCHEMA"},
        )

        # Jinja templating - SnowflakeSqlApiOperator
        # Same Jinja syntax works with SQL API operator
        # Expected return: List of tuples filtered by schema
        api_jinja_params = SnowflakeSqlApiOperator(
            task_id="api_jinja_params",
            conn_id=snowflake_conn_id,
            sql="SELECT table_name, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{{ params.schema }}' LIMIT 5;",
            params={"schema": "INFORMATION_SCHEMA"},
            fetch_results=True,
        )
