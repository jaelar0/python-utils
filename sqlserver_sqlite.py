from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any, Mapping, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


class QueryValidationError(ValueError):
    """Raised when the supplied SQL query is not an allowed read-only SELECT."""


def _validate_select_query(query: str) -> str:
    """
    Ensure the incoming SQL is a single read-only SELECT/CTE query.

    Rules:
    - must start with SELECT or WITH
    - must not contain multiple statements
    - must not contain obvious write/DDL operations

    This is a defensive check only; database permissions should still enforce read-only access.
    """
    if not query or not query.strip():
        raise QueryValidationError("Query cannot be empty.")

    q = query.strip()
    q = re.sub(r";\s*$", "", q)
    lowered = q.lower()

    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise QueryValidationError("Only SELECT or WITH ... SELECT queries are allowed.")

    if ";" in q:
        raise QueryValidationError("Multiple SQL statements are not allowed.")

    forbidden_patterns = [
        r"\binsert\b",
        r"\bupdate\b",
        r"\bdelete\b",
        r"\bdrop\b",
        r"\balter\b",
        r"\btruncate\b",
        r"\bcreate\b",
        r"\bgrant\b",
        r"\brevoke\b",
        r"\bmerge\b",
        r"\bexec\b",
        r"\bexecute\b",
        r"\bcall\b",
        r"\bbackup\b",
        r"\brestore\b",
        r"\bdbcc\b",
    ]
    for pattern in forbidden_patterns:
        if re.search(pattern, lowered):
            raise QueryValidationError(
                f"Disallowed SQL detected. Only read-only SELECT queries are permitted. "
                f"Matched pattern: {pattern}"
            )

    return q


def sqlserver_select_to_sqlite(
    sqlserver_connection_string: str,
    sqlite_path: str | Path,
    source_query: str,
    target_table: str,
    query_params: Optional[Mapping[str, Any]] = None,
    chunk_size: int = 100_000,
    if_exists: str = "replace",
    sqlserver_connect_args: Optional[dict[str, Any]] = None,
    sqlite_timeout: int = 30,
) -> int:
    """
    Execute a read-only SELECT query against SQL Server and load the result into a SQLite table.

    Parameters
    ----------
    sqlserver_connection_string : str
        SQLAlchemy-compatible SQL Server connection string.

        Example using pyodbc:
        'mssql+pyodbc://user:password@server/database?driver=ODBC+Driver+18+for+SQL+Server'

        Example using trusted connection:
        'mssql+pyodbc://@server/database?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes'

    sqlite_path : str | Path
        Path to the target SQLite database file.

    source_query : str
        User-supplied SELECT query. Must be read-only.

    target_table : str
        SQLite table name to create/append into.

    query_params : dict[str, Any], optional
        Bound parameters for the SQL Server query.
        Example: {"start_date": "2026-01-01"}

    chunk_size : int
        Number of rows to fetch per chunk from SQL Server.

    if_exists : str
        One of: 'replace', 'append', 'fail'

    sqlserver_connect_args : dict[str, Any], optional
        Additional SQLAlchemy/driver connection arguments.

    sqlite_timeout : int
        SQLite lock timeout in seconds.

    Returns
    -------
    int
        Total number of rows written to SQLite.

    Raises
    ------
    QueryValidationError
        If the query is not a single read-only SELECT.
    ValueError
        If parameters are invalid.
    RuntimeError
        If data transfer fails.
    """
    if if_exists not in {"replace", "append", "fail"}:
        raise ValueError("if_exists must be one of: 'replace', 'append', 'fail'")

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")

    validated_query = _validate_select_query(source_query)
    sqlite_path = Path(sqlite_path)

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target_table):
        raise ValueError(
            "target_table must be a simple SQL identifier "
            "(letters, numbers, underscores; cannot start with a number)."
        )

    query_params = query_params or {}
    sqlserver_connect_args = sqlserver_connect_args or {}

    logger.info("Starting SQL Server -> SQLite load into table '%s'", target_table)

    engine: Engine = create_engine(
        sqlserver_connection_string,
        future=True,
        pool_pre_ping=True,
        connect_args=sqlserver_connect_args,
    )

    total_rows = 0

    try:
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(sqlite_path, timeout=sqlite_timeout) as sqlite_conn:
            sqlite_conn.execute("PRAGMA foreign_keys = ON;")
            sqlite_conn.execute("PRAGMA journal_mode = WAL;")

            if if_exists == "fail":
                existing = sqlite_conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM sqlite_master
                    WHERE type = 'table' AND lower(name) = lower(?)
                    """,
                    (target_table,),
                ).fetchone()[0]
                if existing:
                    raise ValueError(f"Target table '{target_table}' already exists in SQLite.")

            with engine.connect() as sql_conn:
                chunk_iter = pd.read_sql_query(
                    sql=text(validated_query),
                    con=sql_conn,
                    params=query_params,
                    chunksize=chunk_size,
                )

                first_chunk = True

                for i, chunk in enumerate(chunk_iter, start=1):
                    rows = len(chunk)
                    logger.info("Processing chunk %s with %s rows", i, rows)

                    if rows == 0:
                        continue

                    if first_chunk:
                        chunk.to_sql(
                            name=target_table,
                            con=sqlite_conn,
                            if_exists=if_exists,
                            index=False,
                            method="multi",
                        )
                        first_chunk = False
                    else:
                        chunk.to_sql(
                            name=target_table,
                            con=sqlite_conn,
                            if_exists="append",
                            index=False,
                            method="multi",
                        )

                    total_rows += rows

                sqlite_conn.commit()

        logger.info(
            "Completed SQL Server -> SQLite load into '%s'. Total rows: %s",
            target_table,
            total_rows,
        )
        return total_rows

    except Exception as exc:
        logger.exception("Failed SQL Server -> SQLite load")
        raise RuntimeError(f"Failed to load data from SQL Server to SQLite: {exc}") from exc
    finally:
        engine.dispose()


import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

rows_loaded = sqlserver_select_to_sqlite(
    sqlserver_connection_string=(
        "mssql+pyodbc://user:password@myserver/mydatabase"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&TrustServerCertificate=yes"
    ),
    sqlite_path="data/local_store.db",
    source_query="""
        SELECT
            customer_id,
            order_id,
            order_date,
            total_amount
        FROM dbo.orders
        WHERE order_date >= :start_date
    """,
    query_params={"start_date": "2026-01-01"},
    target_table="orders_2026",
    chunk_size=50000,
    if_exists="replace",
)

print(f"Loaded {rows_loaded} rows")
