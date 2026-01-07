# Snowflake Operators Testing

Airflow project for testing Snowflake operators with a custom `SnowflakeSqlApiOperator` that fetches and returns query results.

## Project Overview

This project explores different Snowflake operators in Apache Airflow:
- **SQLExecuteQueryOperator**: Standard SQL operator (non-deferrable)
- **SnowflakeSqlApiOperator**: REST API-based operator (supports deferrable mode)

The custom `SnowflakeSqlApiOperator` extends the base operator to:
- Fetch and return actual query results (not just query IDs)
- Support `do_xcom_push` parameter
- Support `return_last` parameter for multi-statement queries
- Work in both synchronous and deferrable modes

## Project Structure

```
.
├── dags/
│   ├── example_dag.py              # Main DAG
│   └── test_connection.py          # Connection testing DAG
├── include/
│   └── operators/
│       └── snowflake.py            # Custom SnowflakeSqlApiOperator
├── requirements.txt                # Python dependencies
├── Dockerfile                      # Astro Runtime 13.3.0
├── airflow_settings.yaml           # Local connections/variables
└── snowflake_rsa_key.p8           # Private key for authentication
```

## Custom Operator Features

### SnowflakeSqlApiOperator

Located in `include/operators/snowflake.py`, this custom operator extends the base `SnowflakeSqlApiOperator` to fetch query results:

**Key Features:**
- **`conn_id`** parameter: Uses `conn_id` (not `snowflake_conn_id`) for easy drop-in replacement of `SQLExecuteQueryOperator`
- **`do_xcom_push`** (default: `True`): Fetch and return query results to XComs
- **`return_last`** (default: `True`): Return only the last result from multi-statement queries
- **Deferrable mode support**: Works with `deferrable=True` to free up worker slots

**Usage:**
```python
from include.operators.snowflake import SnowflakeSqlApiOperator

task = SnowflakeSqlApiOperator(
    task_id="query_snowflake",
    sql="SELECT CURRENT_TIMESTAMP();",
    conn_id="snowflake_default",
    deferrable=True,           # Optional: async execution
    do_xcom_push=True,        # Return results
    return_last=True,         # Return last result only
)
```

## Example DAG

The `example_dag` runs 6 tasks sequentially to compare performance:

1. **sql_single**: SQLExecuteQueryOperator (single query)
2. **api_single**: SnowflakeSqlApiOperator (single query, non-deferrable)
3. **api_single_defer**: SnowflakeSqlApiOperator (single query, deferrable)
4. **sql_multi**: SQLExecuteQueryOperator (multi-query)
5. **api_multi**: SnowflakeSqlApiOperator (multi-query, non-deferrable)
6. **api_multi_defer**: SnowflakeSqlApiOperator (multi-query, deferrable)

## Setup

### Prerequisites

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) installed
- Snowflake connection configured with key-pair authentication (Connection ID: `snowflake_default`)
- See [Create a Snowflake Connection in Airflow](https://www.astronomer.io/docs/learn/connections/snowflake) for detailed setup instructions

### Local Development

```bash
# Start Airflow
astro dev start

# Access Airflow UI at http://localhost:8080
# Username: admin
# Password: admin
```

### Astronomer Deployment

```bash
# Wake up deployment (if hibernating)
astro deployment wake-up <DEPLOYMENT_ID> --force

# Deploy
astro deploy <DEPLOYMENT_ID> --force
```

### Authentication

The `SnowflakeSqlApiOperator` requires **key-pair authentication** (RSA public/private key). 

**Key Configuration:**
- **Local development**: Use `private_key_file` path in `airflow_settings.yaml`
- **Astronomer deployment**: Use base64-encoded `private_key_content` in connection extra field
- **Provider version**: Requires `apache-airflow-providers-snowflake>=6.3.0` for base64 encoding

See the [Astronomer Snowflake Connection guide](https://www.astronomer.io/docs/learn/connections/snowflake) for complete setup instructions.

## Testing

### Test Connection
```bash
astro dev run dags test test_connection
```

### Test DAG
```bash
astro dev run dags test example_dag
```

### Run Linter
```bash
ruff check --fix .
ruff format .
```

## Performance Comparison

### SQLExecuteQueryOperator
- Uses JDBC-style connection (`snowflake-connector-python`)
- Blocks worker during query execution
- Simple password authentication
- Good for short queries

### SnowflakeSqlApiOperator (Custom)
- Uses REST API (`SnowflakeSqlApiHook`)
- Supports deferrable mode (frees worker slots)
- Requires key-pair authentication
- Better for long-running queries
- Can handle multiple statements efficiently
- **Drop-in replacement**: Uses `conn_id` parameter for easy migration from `SQLExecuteQueryOperator`

### Deferrable Mode Benefits
- Frees up worker slots during query execution
- Triggerer node handles polling
- Better resource utilization
- Recommended for queries >1 minute

### Migration from SQLExecuteQueryOperator

The custom `SnowflakeSqlApiOperator` is a drop-in replacement using the same `conn_id` parameter:

```python
# Before: SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

task = SQLExecuteQueryOperator(
    task_id="query_snowflake",
    conn_id="snowflake_default",
    sql="SELECT CURRENT_TIMESTAMP();",
)

# After: Custom SnowflakeSqlApiOperator
from include.operators.snowflake import SnowflakeSqlApiOperator

task = SnowflakeSqlApiOperator(
    task_id="query_snowflake",
    conn_id="snowflake_default",  # Same parameter!
    sql="SELECT CURRENT_TIMESTAMP();",
    deferrable=True,              # Add deferrable mode
)
```

## Troubleshooting

### Connection Issues
- Ensure `snowflake_default` connection is configured with key-pair authentication
- Verify Snowflake warehouse is running
- Check account identifier format

### Import Errors
- Ensure `apache-airflow-providers-snowflake` is in `requirements.txt`
- Run `astro dev restart` after adding dependencies

## Resources

- [Create a Snowflake Connection in Airflow](https://www.astronomer.io/docs/learn/connections/snowflake) - Astronomer's complete guide to Snowflake connections
- [Orchestrate Snowflake Queries with Airflow](https://www.astronomer.io/docs/learn/airflow-snowflake) - Full integration tutorial
- [Snowflake SQL API Docs](https://docs.snowflake.com/en/developer-guide/sql-api/intro.html)
- [Airflow Deferrable Operators](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
- [Astronomer Docs](https://www.astronomer.io/docs/)

