from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

import duckdb
import snowflake.connector


def load_snowflake_to_raw_duckdb(
    query: str,
    table_name: str,
    duckdb_path: str,
    *,
    schema_name: str = "raw",
    if_exists: str = "replace",
    sf_user: Optional[str] = None,
    sf_password: Optional[str] = None,
    sf_account: Optional[str] = None,
    sf_warehouse: Optional[str] = None,
    sf_database: Optional[str] = None,
    sf_schema: Optional[str] = None,
    sf_role: Optional[str] = None,
) -> None:
    """
    Execute a Snowflake SELECT query and load the results into a DuckDB table.

    Parameters
    ----------
    query : str
        Snowflake SELECT query to execute.
    table_name : str
        Target DuckDB table name.
    duckdb_path : str
        Path to the raw DuckDB database file.
    schema_name : str, default "raw"
        Target schema in DuckDB.
    if_exists : str, default "replace"
        Supported values:
        - "replace": drop and recreate table
        - "append": insert into existing table
        - "fail": raise error if table already exists
    sf_user, sf_password, sf_account, sf_warehouse, sf_database, sf_schema, sf_role
        Snowflake connection fields. If omitted, environment variables are used.

    Environment Variables
    ---------------------
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA
    SNOWFLAKE_ROLE
    """
    if not query.strip().lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")

    db_path = Path(duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    sf_user = sf_user or os.getenv("SNOWFLAKE_USER")
    sf_password = sf_password or os.getenv("SNOWFLAKE_PASSWORD")
    sf_account = sf_account or os.getenv("SNOWFLAKE_ACCOUNT")
    sf_warehouse = sf_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    sf_database = sf_database or os.getenv("SNOWFLAKE_DATABASE")
    sf_schema = sf_schema or os.getenv("SNOWFLAKE_SCHEMA")
    sf_role = sf_role or os.getenv("SNOWFLAKE_ROLE")

    required = {
        "SNOWFLAKE_USER": sf_user,
        "SNOWFLAKE_PASSWORD": sf_password,
        "SNOWFLAKE_ACCOUNT": sf_account,
        "SNOWFLAKE_WAREHOUSE": sf_warehouse,
        "SNOWFLAKE_DATABASE": sf_database,
        "SNOWFLAKE_SCHEMA": sf_schema,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing Snowflake connection values: {', '.join(missing)}")

    full_table_name = f"{schema_name}.{table_name}"

    sf_conn = None
    sf_cur = None
    ddb_conn = None

    try:
        sf_conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_schema,
            role=sf_role,
        )
        sf_cur = sf_conn.cursor()
        sf_cur.execute(query)

        ddb_conn = duckdb.connect(str(db_path))
        ddb_conn.execute(f"create schema if not exists {schema_name}")

        table_exists = ddb_conn.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = ? and table_name = ?
            """,
            [schema_name, table_name],
        ).fetchone()[0] > 0

        if if_exists == "fail" and table_exists:
            raise ValueError(f"Table already exists: {full_table_name}")

        if if_exists == "replace":
            ddb_conn.execute(f"drop table if exists {full_table_name}")

        wrote_any = False

        for i, batch_df in enumerate(sf_cur.fetch_pandas_batches(), start=1):
            if batch_df is None or batch_df.empty:
                continue

            wrote_any = True
            temp_view_name = f"_sf_batch_{i}"
            ddb_conn.register(temp_view_name, batch_df)

            if if_exists == "replace":
                if i == 1:
                    ddb_conn.execute(
                        f"create table {full_table_name} as "
                        f"select * from {temp_view_name}"
                    )
                else:
                    ddb_conn.execute(
                        f"insert into {full_table_name} "
                        f"select * from {temp_view_name}"
                    )

            elif if_exists == "append":
                if i == 1 and not table_exists:
                    ddb_conn.execute(
                        f"create table {full_table_name} as "
                        f"select * from {temp_view_name}"
                    )
                else:
                    ddb_conn.execute(
                        f"insert into {full_table_name} "
                        f"select * from {temp_view_name}"
                    )

            elif if_exists == "fail":
                if i == 1:
                    ddb_conn.execute(
                        f"create table {full_table_name} as "
                        f"select * from {temp_view_name}"
                    )
                else:
                    ddb_conn.execute(
                        f"insert into {full_table_name} "
                        f"select * from {temp_view_name}"
                    )

            else:
                raise ValueError("if_exists must be one of: replace, append, fail")

            ddb_conn.unregister(temp_view_name)

        if not wrote_any:
            # Create an empty table with the right structure if query returned no rows
            empty_df = sf_cur.fetch_pandas_all()
            ddb_conn.register("_sf_empty_df", empty_df)

            if if_exists == "replace":
                ddb_conn.execute(
                    f"create table {full_table_name} as select * from _sf_empty_df"
                )
            elif if_exists == "append":
                if not table_exists:
                    ddb_conn.execute(
                        f"create table {full_table_name} as select * from _sf_empty_df"
                    )
            elif if_exists == "fail":
                ddb_conn.execute(
                    f"create table {full_table_name} as select * from _sf_empty_df"
                )

            ddb_conn.unregister("_sf_empty_df")

    finally:
        if ddb_conn is not None:
            ddb_conn.close()
        if sf_cur is not None:
            sf_cur.close()
        if sf_conn is not None:
            sf_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Snowflake SELECT query and load results into a raw DuckDB table."
    )
    parser.add_argument(
        "--query-file",
        required=True,
        help="Path to a .sql file containing the Snowflake SELECT query",
    )
    parser.add_argument(
        "--table-name",
        required=True,
        help="Target DuckDB table name",
    )
    parser.add_argument(
        "--duckdb-path",
        required=True,
        help="Path to the raw DuckDB database file",
    )
    parser.add_argument(
        "--schema-name",
        default="raw",
        help="Target DuckDB schema name (default: raw)",
    )
    parser.add_argument(
        "--if-exists",
        default="replace",
        choices=["replace", "append", "fail"],
        help="What to do if the table exists (default: replace)",
    )

    # Optional explicit Snowflake args; falls back to env vars if omitted
    parser.add_argument("--sf-user", default=None, help="Snowflake user")
    parser.add_argument("--sf-password", default=None, help="Snowflake password")
    parser.add_argument("--sf-account", default=None, help="Snowflake account")
    parser.add_argument("--sf-warehouse", default=None, help="Snowflake warehouse")
    parser.add_argument("--sf-database", default=None, help="Snowflake database")
    parser.add_argument("--sf-schema", default=None, help="Snowflake schema")
    parser.add_argument("--sf-role", default=None, help="Snowflake role")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    query_path = Path(args.query_file)
    if not query_path.exists():
        raise FileNotFoundError(f"SQL file not found: {query_path}")

    query = query_path.read_text(encoding="utf-8").strip()

    load_snowflake_to_raw_duckdb(
        query=query,
        table_name=args.table_name,
        duckdb_path=args.duckdb_path,
        schema_name=args.schema_name,
        if_exists=args.if_exists,
        sf_user=args.sf_user,
        sf_password=args.sf_password,
        sf_account=args.sf_account,
        sf_warehouse=args.sf_warehouse,
        sf_database=args.sf_database,
        sf_schema=args.sf_schema,
        sf_role=args.sf_role,
    )

    print(
        f"Loaded Snowflake query results from '{args.query_file}' into "
        f"'{args.schema_name}.{args.table_name}' in DuckDB database '{args.duckdb_path}'."
    )


if __name__ == "__main__":
    main()
