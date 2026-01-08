# Snowflake Operators Testing

Airflow project for testing Snowflake operators with a custom `SnowflakeSqlApiOperator` that fetches and returns query results.

## Project Overview

This project compares different Snowflake operators in Apache Airflow:
- **SQLExecuteQueryOperator**: Standard SQL operator using Snowflake Python connector
- **SnowflakeSqlApiOperator**: REST API-based operator (supports deferrable mode)

The custom `SnowflakeSqlApiOperator` extends the base operator to:
- Fetch and return actual query results (list of tuples, same format as SQLExecuteQueryOperator)
- Support `fetch_results` parameter to toggle between query IDs and actual results
- Support `return_last` parameter for multi-statement queries
- Work in both synchronous and deferrable modes

## Project Structure

```
.
├── dags/
│   ├── example_dag.py              # Main DAG with all test scenarios
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

Located in `include/operators/snowflake.py`, this custom operator extends the base `SnowflakeSqlApiOperator`:

**Parameters:**
- **`conn_id`**: Snowflake connection ID (uses `conn_id` instead of `snowflake_conn_id` for SQLExecuteQueryOperator compatibility)
- **`fetch_results`** (default: `False`): When `True`, fetches and returns query results as list of tuples
- **`return_last`** (default: `True`): When `True`, returns only the last result from multi-statement queries
- **`deferrable`** (default: `False`): When `True`, uses async execution to free worker slots

**Usage:**
```python
from include.operators.snowflake import SnowflakeSqlApiOperator

# Default behavior (returns query IDs, same as base operator)
task = SnowflakeSqlApiOperator(
    task_id="query_snowflake",
    sql="SELECT CURRENT_TIMESTAMP();",
    conn_id="snowflake_default",
    deferrable=True,
)

# Fetch results (returns list of tuples like SQLExecuteQueryOperator)
task_with_results = SnowflakeSqlApiOperator(
    task_id="query_with_results",
    sql="SELECT CURRENT_TIMESTAMP();",
    conn_id="snowflake_default",
    fetch_results=True,
)
```

## Example DAG

The `example_dag` contains 4 task groups testing different scenarios:

### Task Groups

1. **single_query**: Single query with all `fetch_results` x `deferrable` combinations
2. **multi_query**: Multi-statement queries with all `return_last` x `fetch_results` x `deferrable` combinations
3. **sql_execute_query**: SQLExecuteQueryOperator comparison tasks
4. **parameterized_queries**: Parameter binding and Jinja templating examples

### Parameterized Queries

The DAG demonstrates different parameter styles:

| Operator | Style | Syntax | Example |
|----------|-------|--------|---------|
| SQLExecuteQueryOperator | Positional | `%s` | `parameters=["value"]` |
| SQLExecuteQueryOperator | Named | `%(name)s` | `parameters={"name": "value"}` |
| SnowflakeSqlApiOperator | Bindings | `?` | `bindings={"1": {"type": "TEXT", "value": "..."}}` |
| Both | Jinja | `{{ params.name }}` | `params={"name": "value"}` |

**Note:** `%` style parameters are NOT supported with `SnowflakeSqlApiOperator` - use `?` with bindings only.

## Setup

### Prerequisites

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) installed
- Snowflake connection configured with key-pair authentication (Connection ID: `snowflake_default`)
- See [Create a Snowflake Connection in Airflow](https://www.astronomer.io/docs/learn/connections/snowflake) for setup instructions

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

## Testing

```bash
# Test connection
astro dev run dags test test_connection

# Test example DAG
astro dev run dags test example_dag

# Run linter
ruff check --fix .
ruff format .
```

## Operator Comparison

| Feature | SQLExecuteQueryOperator | SnowflakeSqlApiOperator (Custom) |
|---------|------------------------|----------------------------------|
| Connection | Python connector (JDBC-style) | REST API |
| Deferrable | No | Yes |
| Authentication | Password or key-pair | Key-pair only |
| Parameters | `%s`, `%(name)s` | `?` with bindings |
| Output format | List of tuples | List of tuples (with `fetch_results=True`) |
| Best for | Short queries | Long-running queries |

### Migration from SQLExecuteQueryOperator

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
    fetch_results=True,           # Get results like SQLExecuteQueryOperator
    deferrable=True,              # Optional: async execution
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

- [Create a Snowflake Connection in Airflow](https://www.astronomer.io/docs/learn/connections/snowflake)
- [Orchestrate Snowflake Queries with Airflow](https://www.astronomer.io/docs/learn/airflow-snowflake)
- [Snowflake SQL API Docs](https://docs.snowflake.com/en/developer-guide/sql-api/intro.html)
- [Airflow Deferrable Operators](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
