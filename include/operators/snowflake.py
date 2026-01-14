"""Custom Snowflake operators."""

from typing import Any

from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeSqlApiOperator as SnowflakeSqlApiOperatorBase,
)
from airflow.utils.context import Context


class SnowflakeSqlApiOperator(SnowflakeSqlApiOperatorBase):
    """
    SnowflakeSqlApiOperator with optional result fetching.

    Extends SnowflakeSqlApiOperator to optionally fetch query results.
    By default, returns query IDs (same as base operator). When fetch_results=True,
    fetches and returns actual query results.

    :param conn_id: Snowflake connection ID (mapped to snowflake_conn_id)
    :param fetch_results: Whether to fetch and return query results (default: False)
    :param return_last: Whether to return only the last query result when multiple
        statements are executed (default: True)
    """

    def __init__(
        self,
        *,
        conn_id: str = "snowflake_default",
        fetch_results: bool = False,
        return_last: bool = True,
        **kwargs,
    ) -> None:
        # Map conn_id to snowflake_conn_id for base class
        super().__init__(snowflake_conn_id=conn_id, **kwargs)
        self.fetch_results = fetch_results
        self.return_last = return_last

    def _fetch_results(self) -> Any:
        """
        Fetch query results from Snowflake.

        For multi-statement queries, first retrieves individual statement handles
        from the status response, then fetches results for each statement.

        Returns results as list of tuples to match SQLExecuteQueryOperator format.
        """
        hook = SnowflakeSqlApiHook(
            snowflake_conn_id=self.snowflake_conn_id,
            token_life_time=self.token_life_time,
            token_renewal_delta=self.token_renewal_delta,
        )

        # Collect all statement handles (may differ from query_ids for multi-statement)
        all_handles = []
        for qid in self.query_ids:
            # Get status to retrieve statement_handles for multi-statement queries
            status = hook.get_sql_api_query_status(qid)
            handles = status.get("statement_handles", [])
            if handles:
                all_handles.extend(handles)
            else:
                # Single statement - use the query ID directly
                all_handles.append(qid)

        # Fetch results for each statement handle
        results = []
        for handle in all_handles:
            data = hook.get_result_from_successful_sql_api_query(handle)
            # Convert list of dicts to list of tuples to match SQLExecuteQueryOperator
            rows = [tuple(row.values()) for row in data]
            results.append(rows)

        # Return last result only if return_last is True and multiple statements
        if self.return_last and len(results) > 1:
            return results[-1]
        # For single query, return just the result, not a list
        if len(results) == 1:
            return results[0]
        return results

    def execute(self, context: Context) -> Any:
        """
        Execute SQL using Snowflake SQL API.

        In deferrable mode, if the query is deferred, the base class raises
        ``TaskDeferred`` and ``execute_complete`` handles the result fetching.
        If we reach the code after ``super().execute()``, it means the query
        completed (either synchronously or deferrable mode didn't defer).
        """
        super().execute(context)

        if not self.fetch_results:
            return self.query_ids

        return self._fetch_results()

    def execute_complete(
        self, context: Context, event: dict[str, str | list[str]] | None = None
    ) -> Any:
        """Callback for deferrable mode."""
        super().execute_complete(context, event)

        if not self.fetch_results:
            return self.query_ids

        return self._fetch_results()
