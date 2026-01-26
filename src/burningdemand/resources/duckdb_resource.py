import duckdb
import pandas as pd
from pathlib import Path
from dagster import ConfigurableResource
from pydantic import Field


class DuckDBResource(ConfigurableResource):
    db_path: str = Field(default="./data/burningdemand.duckdb")
    schema_path: str = Field(default="src/burningdemand/schema/duckdb.sql")

    def _get_conn(self):
        """Internal helper to ensure path exists and get connection."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(self.db_path)

    def setup_for_execution(self, context) -> None:
        """
        Initializes the database by creating schemas and running the SQL script.
        """
        with self._get_conn() as conn:
            # 1. Ensure schemas exist first
            for schema in ["bronze", "silver", "gold"]:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            # 2. Run the main schema SQL
            schema_file = Path(self.schema_path)
            if not schema_file.exists():
                raise FileNotFoundError(f"Schema file missing: {schema_file}")

            # Use execute on the full text of the SQL file
            conn.execute(schema_file.read_text())

    def query_df(self, sql: str, params=None) -> pd.DataFrame:
        with self._get_conn() as conn:
            return conn.execute(sql, params or []).df()

    def execute(self, sql: str, params=None) -> None:
        with self._get_conn() as conn:
            conn.execute(sql, params or [])

    def upsert_df(
        self, schema: str, table: str, df: pd.DataFrame, columns: list[str]
    ) -> int:
        """
        Uses DuckDB's native 'INSERT OR REPLACE' into a specific schema.table.
        Assumes the table has a PRIMARY KEY defined in your schema.sql.
        """
        if df is None or df.empty:
            return 0

        # Construct full table path (e.g., bronze.items)
        full_table_path = f"{schema}.{table}"

        # Filter DF to requested columns and prepare SQL string
        upsert_data = df[columns]
        cols_str = ", ".join(columns)

        with self._get_conn() as conn:
            # Register the dataframe as a temporary view
            conn.register("tmp_df_view", upsert_data)

            # Native UPSERT: Replaces rows with matching Primary Keys
            # Specifying (columns) ensures we don't hit "count mismatch" errors
            query = f"""
                INSERT OR REPLACE INTO {full_table_path} ({cols_str}) 
                SELECT {cols_str} FROM tmp_df_view
            """
            conn.execute(query)

        return len(df)
