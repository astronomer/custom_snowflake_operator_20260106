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
    When do_xcom_push=True, returns actual results; otherwise returns execution ID.

    :param conn_id: Snowflake connection ID (mapped to snowflake_conn_id)
    :param do_xcom_push: Whether to fetch and return query results (default: True)
    :param return_last: Whether to return only the last query result when multiple
        statements are executed (default: True)
    """

    def __init__(
        self,
        *,
        conn_id: str = "snowflake_default",
        do_xcom_push: bool = True,
        return_last: bool = True,
        **kwargs,
    ) -> None:
        # Map conn_id to snowflake_conn_id for base class
        super().__init__(snowflake_conn_id=conn_id, **kwargs)
        self.do_xcom_push = do_xcom_push
        self.return_last = return_last

    def _fetch_results(self) -> Any:
        """Fetch query results from Snowflake."""
        hook = SnowflakeSqlApiHook(
            snowflake_conn_id=self.snowflake_conn_id,
            token_life_time=self.token_life_time,
            token_renewal_delta=self.token_renewal_delta,
        )

        results = [hook.get_sql_api_query_status(qid) for qid in self.query_ids]

        # Return last result only if return_last is True and multiple statements were executed
        if self.return_last and len(results) > 1:
            return results[-1]
        return results

    def execute(self, context: Context) -> Any:
        """Execute SQL using Snowflake SQL API."""
        super().execute(context)

        if not self.do_xcom_push or self.deferrable:
            return self.query_ids

        return self._fetch_results()

    def execute_complete(
        self, context: Context, event: dict[str, str | list[str]] | None = None
    ) -> Any:
        """Callback for deferrable mode."""
        super().execute_complete(context, event)

        if not self.do_xcom_push:
            return self.query_ids

        return self._fetch_results()
