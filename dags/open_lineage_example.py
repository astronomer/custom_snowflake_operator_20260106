"""
OpenLineage Support Examples for Snowflake.

This DAG demonstrates different approaches to enable OpenLineage lineage extraction
with Snowflake queries in Apache Airflow.

OpenLineage Support Methods
---------------------------

1. **Built-in Operators (Recommended)**: Use SQLExecuteQueryOperator or
   SnowflakeSqlApiOperator which have native OpenLineage support via their hooks.
   See: https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/supported_classes.html

2. **Inlets/Outlets**: For @task decorated functions, declare inlets/outlets
   to manually specify lineage datasets.

3. **LineageConnection Wrapper**: For existing snowflake.connector code, wrap the
   connection with LineageConnection to automatically parse SQL and emit lineage.
   Uses sqlglot for SQL parsing and openlineage-python for event emission.

See: https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/developer.html
"""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.lineage.entities import Table
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.operators.snowflake import SnowflakeSqlApiOperator

SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_NAMESPACE = "snowflake://my_account"
QUERY = "SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 5;"


# Extracting crendentials from Airflow connection to pass to snowflake.connector
# This is to avoid hardcoding credentials in the DAG even if we are 
# using snowflake.connector directly inside the Python Operator
def get_snowflake_conn_params(conn_id: str = SNOWFLAKE_CONN_ID) -> dict:
    """Get Snowflake connection parameters from Airflow connection."""
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson

    return {
        "user": conn.login,
        "password": conn.password,
        "account": extra.get("account", ""),
        "warehouse": extra.get("warehouse"),
        "database": extra.get("database"),
        "schema": conn.schema,
        "role": extra.get("role"),
        "private_key_content": extra.get("private_key_content"),
    }


DAG_DOC = """
## OpenLineage Support Examples

This DAG demonstrates different approaches to enable OpenLineage lineage extraction
with Snowflake queries in Apache Airflow.

### Methods Covered

1. **Built-in Operators** - Use SQLExecuteQueryOperator or SnowflakeSqlApiOperator
   with native OpenLineage support
2. **Inlets/Outlets** - Manually declare lineage for @task decorated functions
3. **LineageConnection Wrapper** - Wrap snowflake.connector for automatic lineage

See: [OpenLineage Developer Guide](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/developer.html)
"""

with DAG(
    "openlineage_examples",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    doc_md=DAG_DOC,
):
    # Method 1: Built-in Operators with Native OpenLineage Support
    sql_with_openlineage = SQLExecuteQueryOperator(
        task_id="sql_with_openlineage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=QUERY,
        doc_md="""
**Method 1: SQLExecuteQueryOperator (Recommended)**

Uses hooks that implement OpenLineage methods. Lineage is automatically
extracted including SQL parsing for inputs/outputs.

[Supported Classes](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/supported_classes.html)
        """,
    )

    api_with_openlineage = SnowflakeSqlApiOperator(
        task_id="api_with_openlineage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=QUERY,
        fetch_results=True,
        doc_md="""
**Method 1: SnowflakeSqlApiOperator**

Has OpenLineage support via SnowflakeSqlApiHook. Lineage is automatically
extracted from executed SQL queries.
        """,
    )

    # Method 2: Using Inlets/Outlets with @task Decorator
    # Declare lineage manually - input from system table, output to temp table
    input_table = Table(
        cluster=SNOWFLAKE_NAMESPACE,
        database="INFORMATION_SCHEMA",
        name="TABLES",
    )
    output_table = Table(
        cluster=SNOWFLAKE_NAMESPACE,
        database="TEMP",
        name="TABLE_SUMMARY",
    )

    @task(
        task_id="task_with_inlets_outlets",
        inlets=[input_table],
        outlets=[output_table],
        doc_md="""
**Method 2: Inlets/Outlets with @task Decorator**

Manually declare lineage by specifying `inlets` (inputs) and `outlets` (outputs).
OpenLineage will report these as input/output datasets even though the actual
query execution is a "black box".

Use when you know the tables involved but can't rely on automatic SQL parsing.
        """,
    )
    def task_with_lineage():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        # Use SnowflakeHook instead of direct connector for better integration
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        result = hook.get_first(QUERY)
        return result

    task_with_lineage()

    # Method 3: LineageConnection Wrapper for Existing snowflake.connector Code
    @task(
        task_id="connector_with_lineage_wrapper",
        doc_md="""
**Method 3: LineageConnection Wrapper**

Production-grade solution for existing `snowflake.connector` code.
Wrap your connection with `LineageConnection` to automatically:

1. Intercept `execute()` calls
2. Parse SQL using `sqlglot` to extract table references
3. Emit OpenLineage events with inputs/outputs

Minimal code change required - just wrap the connection.
        """,
    )
    def connector_with_lineage():
        import snowflake.connector

        from include.lineage.snowflake_lineage import LineageConnection

        # Get connection params from Airflow connection
        conn_params = get_snowflake_conn_params()

        # Handle key-pair auth if private_key_content is set
        connect_args = {
            "user": conn_params["user"],
            "account": conn_params["account"],
            "warehouse": conn_params["warehouse"],
            "database": conn_params["database"],
            "schema": conn_params["schema"],
            "role": conn_params["role"],
        }

        if conn_params.get("private_key_content"):
            import base64

            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            key_bytes = base64.b64decode(conn_params["private_key_content"])
            private_key = serialization.load_pem_private_key(
                key_bytes, password=None, backend=default_backend()
            )
            connect_args["private_key"] = private_key
        else:
            connect_args["password"] = conn_params["password"]

        # Wrap existing connection with LineageConnection
        raw_conn = snowflake.connector.connect(**connect_args)
        conn = LineageConnection(raw_conn, job_name="my_etl_job")

        # Execute queries as normal - lineage is extracted automatically
        # Using INFORMATION_SCHEMA.TABLES which is always available
        query = "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES LIMIT 5"
        cursor = conn.cursor().execute(query)
        result = cursor.fetchone()

        # Access extracted lineage from the cursor
        import logging

        logging.info(f"Extracted lineage: {cursor.last_query_lineage}")
        # Expected: {"inputs": ["INFORMATION_SCHEMA.TABLES"], "outputs": []}

        conn.close()
        return result

    connector_with_lineage()

    @task(
        task_id="connector_with_context_manager",
        doc_md="""
**Method 3: Context Manager (Recommended for new code)**

`LineageConnection.connect()` provides a context manager for cleaner code.
Handles connection lifecycle automatically and emits lineage events.

Demonstrates CREATE TEMP TABLE with inputs and outputs detected.
        """,
    )
    def connector_with_context_manager():
        import logging

        import snowflake.connector

        from include.lineage.snowflake_lineage import LineageConnection

        # Get connection params from Airflow connection
        conn_params = get_snowflake_conn_params()

        # Build connection args (same pattern as above)
        connect_args = {
            "user": conn_params["user"],
            "account": conn_params["account"],
            "warehouse": conn_params["warehouse"],
            "database": conn_params["database"],
            "schema": conn_params["schema"],
            "role": conn_params["role"],
        }

        if conn_params.get("private_key_content"):
            import base64

            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            key_bytes = base64.b64decode(conn_params["private_key_content"])
            private_key = serialization.load_pem_private_key(
                key_bytes, password=None, backend=default_backend()
            )
            connect_args["private_key"] = private_key
        else:
            connect_args["password"] = conn_params["password"]

        # Wrap connection with LineageConnection
        raw_conn = snowflake.connector.connect(**connect_args)
        conn = LineageConnection(
            raw_conn, job_name="my_etl_job", namespace=SNOWFLAKE_NAMESPACE
        )

        try:
            # CREATE TEMP TABLE demonstrates both inputs and outputs
            # Temp tables don't require special permissions and auto-cleanup
            ctas_query = """
                CREATE OR REPLACE TEMPORARY TABLE TEMP_TABLE_SUMMARY AS
                SELECT t.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON t.TABLE_NAME = c.TABLE_NAME
                LIMIT 100
            """
            cursor = conn.cursor().execute(ctas_query)

            logging.info(f"Extracted lineage: {cursor.last_query_lineage}")
            # Expected: inputs=[TABLES, COLUMNS], outputs=[TEMP_TABLE_SUMMARY]

            # Query the temp table to verify
            result = (
                conn.cursor()
                .execute("SELECT COUNT(*) FROM TEMP_TABLE_SUMMARY")
                .fetchone()
            )
            return result
        finally:
            conn.close()

    connector_with_context_manager()

    # Task dependencies for visualization
    sql_with_openlineage >> api_with_openlineage
