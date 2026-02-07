import duckdb
import pandas as pd
from pathlib import Path
from typing import Any, Optional

from dagster import ConfigurableResource
from pydantic import Field, PrivateAttr


class DuckDBResource(ConfigurableResource):
    db_path: str = Field(default="./data/burningdemand.duckdb")
    schema_path: str = Field(default="src/burningdemand/schema/duckdb.sql")

    _conn: Optional[Any] = PrivateAttr(default=None)

    def _get_conn(self):
        """Create a new connection (used when no run-scoped connection is set)."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(self.db_path)

    def _conn_or_new(self):
        """Use run-scoped connection if set, otherwise open a new one (fallback)."""
        if self._conn is not None:
            return self._conn
        return self._get_conn()

    def setup_for_execution(self, context) -> None:
        """Open one connection for the run and ensure schema exists."""
        self._conn = self._get_conn()
        conn = self._conn
        for schema in ["bronze", "silver", "gold"]:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        schema_file = Path(self.schema_path)
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file missing: {schema_file}")
        conn.execute(schema_file.read_text())

    def teardown_after_execution(self, context) -> None:
        """Close the run-scoped connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def query_df(self, sql: str, params=None) -> pd.DataFrame:
        conn = self._conn_or_new()
        return conn.execute(sql, params or []).df()

    def execute(self, sql: str, params=None) -> None:
        conn = self._conn_or_new()
        conn.execute(sql, params or [])

    def upsert_df(
        self, schema: str, table: str, df: pd.DataFrame, columns: list[str]
    ) -> int:
        if df is None or df.empty:
            return 0
        full_table_path = f"{schema}.{table}"
        upsert_data = df[columns].copy()
        for col in upsert_data.columns:
            if pd.api.types.is_string_dtype(upsert_data[col]):
                upsert_data[col] = upsert_data[col].astype(object)
        cols_str = ", ".join(columns)
        conn = self._conn_or_new()
        conn.register("tmp_df_view", upsert_data)
        query = f"""
            INSERT OR REPLACE INTO {full_table_path} ({cols_str})
            SELECT {cols_str} FROM tmp_df_view
        """
        conn.execute(query)
        return len(df)
