"""
Snowflake Lineage Wrapper for OpenLineage Integration.

This module provides a drop-in wrapper for snowflake.connector that automatically
extracts lineage from SQL queries and emits OpenLineage events.

Usage
-----
Minimal change to existing code - just wrap your connection:

    # Before
    conn = snowflake.connector.connect(...)
    result = conn.cursor().execute(query).fetchone()

    # After
    from include.lineage.snowflake_lineage import LineageConnection

    conn = LineageConnection(snowflake.connector.connect(...))
    result = conn.cursor().execute(query).fetchone()

Or use the context manager for automatic cleanup:

    with LineageConnection.connect(**conn_params) as conn:
        result = conn.cursor().execute(query).fetchone()

The wrapper will:
1. Parse SQL queries using sqlglot to extract table references
2. Emit OpenLineage events with input/output datasets
3. Pass through all results unchanged
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection
    from snowflake.connector.cursor import SnowflakeCursor

logger = logging.getLogger(__name__)


def parse_sql_tables(sql: str, dialect: str = "snowflake") -> dict[str, list[str]]:
    """
    Parse SQL query to extract input and output table references.

    Uses sqlglot for SQL parsing. Falls back gracefully if parsing fails.

    :param sql: SQL query string
    :param dialect: SQL dialect for parsing (default: snowflake)
    :return: Dict with 'inputs' and 'outputs' lists of table names
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        logger.warning("sqlglot not installed. Install with: pip install sqlglot")
        return {"inputs": [], "outputs": []}

    inputs = []
    outputs = []

    try:
        parsed = sqlglot.parse(sql, dialect=dialect)

        for statement in parsed:
            if statement is None:
                continue

            # Find output tables (INSERT, CREATE, MERGE targets)
            if isinstance(statement, (exp.Insert, exp.Create, exp.Merge)):
                for table in statement.find_all(exp.Table):
                    table_name = _format_table_name(table)
                    if table_name and table_name not in outputs:
                        # First table in INSERT/CREATE is typically the target
                        outputs.append(table_name)
                        break

            # Find input tables (FROM, JOIN clauses)
            for table in statement.find_all(exp.Table):
                table_name = _format_table_name(table)
                if (
                    table_name
                    and table_name not in inputs
                    and table_name not in outputs
                ):
                    inputs.append(table_name)

    except Exception as e:
        logger.debug(f"Failed to parse SQL for lineage: {e}")

    return {"inputs": inputs, "outputs": outputs}


def _format_table_name(table) -> str | None:
    """Format table expression to fully qualified name."""
    parts = []
    if table.catalog:
        parts.append(table.catalog)
    if table.db:
        parts.append(table.db)
    if table.name:
        parts.append(table.name)
    return ".".join(parts) if parts else None


def emit_openlineage_event(
    job_name: str,
    inputs: list[str],
    outputs: list[str],
    namespace: str = "snowflake",
) -> None:
    """
    Emit an OpenLineage event with the given inputs and outputs.

    :param job_name: Name of the job/task
    :param inputs: List of input table names
    :param outputs: List of output table names
    :param namespace: OpenLineage namespace (default: snowflake)
    """
    try:
        from openlineage.client import OpenLineageClient
        from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
        from openlineage.client.uuid import generate_new_uuid
    except ImportError:
        logger.debug("openlineage-python not installed. Lineage events not emitted.")
        return

    try:
        client = OpenLineageClient()

        input_datasets = [
            Dataset(namespace=namespace, name=table_name) for table_name in inputs
        ]
        output_datasets = [
            Dataset(namespace=namespace, name=table_name) for table_name in outputs
        ]

        run = Run(runId=str(generate_new_uuid()))
        job = Job(namespace=namespace, name=job_name)

        # Emit COMPLETE event with datasets
        event = RunEvent(
            eventType=RunState.COMPLETE,
            run=run,
            job=job,
            inputs=input_datasets,
            outputs=output_datasets,
        )

        client.emit(event)
        logger.debug(f"Emitted OpenLineage event: inputs={inputs}, outputs={outputs}")

    except Exception as e:
        logger.debug(f"Failed to emit OpenLineage event: {e}")


class LineageCursor:
    """
    Wrapper around SnowflakeCursor that extracts lineage from executed queries.

    Intercepts execute() calls to parse SQL and emit OpenLineage events.
    """

    def __init__(
        self,
        cursor: "SnowflakeCursor",
        job_name: str = "snowflake_query",
        namespace: str = "snowflake",
        emit_lineage: bool = True,
    ) -> None:
        self._cursor = cursor
        self._job_name = job_name
        self._namespace = namespace
        self._emit_lineage = emit_lineage
        self._last_query_lineage: dict[str, list[str]] = {"inputs": [], "outputs": []}

    def execute(self, sql: str, *args, **kwargs) -> "LineageCursor":
        """Execute SQL and extract lineage."""
        # Parse SQL for lineage before execution
        if self._emit_lineage:
            self._last_query_lineage = parse_sql_tables(sql)

        # Execute the actual query
        self._cursor.execute(sql, *args, **kwargs)

        # Emit lineage event after successful execution
        if self._emit_lineage and (
            self._last_query_lineage["inputs"] or self._last_query_lineage["outputs"]
        ):
            emit_openlineage_event(
                job_name=self._job_name,
                inputs=self._last_query_lineage["inputs"],
                outputs=self._last_query_lineage["outputs"],
                namespace=self._namespace,
            )

        return self

    @property
    def last_query_lineage(self) -> dict[str, list[str]]:
        """Get the lineage extracted from the last executed query."""
        return self._last_query_lineage

    def __getattr__(self, name: str) -> Any:
        """Proxy all other attributes to the underlying cursor."""
        return getattr(self._cursor, name)


class LineageConnection:
    """
    Wrapper around SnowflakeConnection that provides lineage-aware cursors.

    Drop-in replacement for snowflake.connector connections with automatic
    OpenLineage event emission.
    """

    def __init__(
        self,
        connection: "SnowflakeConnection",
        job_name: str = "snowflake_query",
        namespace: str = "snowflake",
        emit_lineage: bool = True,
    ) -> None:
        """
        Wrap a Snowflake connection with lineage support.

        :param connection: Existing snowflake.connector connection
        :param job_name: Name for OpenLineage job (default: snowflake_query)
        :param namespace: OpenLineage namespace (default: snowflake)
        :param emit_lineage: Whether to emit lineage events (default: True)
        """
        self._connection = connection
        self._job_name = job_name
        self._namespace = namespace
        self._emit_lineage = emit_lineage

    @classmethod
    @contextmanager
    def connect(
        cls,
        job_name: str = "snowflake_query",
        namespace: str = "snowflake",
        emit_lineage: bool = True,
        **connection_params,
    ):
        """
        Context manager for creating a lineage-aware Snowflake connection.

        :param job_name: Name for OpenLineage job
        :param namespace: OpenLineage namespace
        :param emit_lineage: Whether to emit lineage events
        :param connection_params: Parameters passed to snowflake.connector.connect()
        """
        import snowflake.connector

        conn = snowflake.connector.connect(**connection_params)
        wrapped = cls(
            conn, job_name=job_name, namespace=namespace, emit_lineage=emit_lineage
        )
        try:
            yield wrapped
        finally:
            conn.close()

    def cursor(self, *args, **kwargs) -> LineageCursor:
        """Return a lineage-aware cursor."""
        cursor = self._connection.cursor(*args, **kwargs)
        return LineageCursor(
            cursor,
            job_name=self._job_name,
            namespace=self._namespace,
            emit_lineage=self._emit_lineage,
        )

    def close(self) -> None:
        """Close the underlying connection."""
        self._connection.close()

    def __getattr__(self, name: str) -> Any:
        """Proxy all other attributes to the underlying connection."""
        return getattr(self._connection, name)

    def __enter__(self) -> "LineageConnection":
        return self

    def __exit__(self, *args) -> None:
        self.close()
