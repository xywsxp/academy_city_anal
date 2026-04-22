"""ClickHouse connection helper for academy_city_anal analysis.

Usage:
    from utils.ch_client import get_client, query_df

    client = get_client()                         # uses defaults / .env
    df = query_df("SELECT * FROM test_sol_transfer_local LIMIT 10")

Environment variables (or .env file in repo root):
    CH_HOST     ClickHouse host          (default: localhost)
    CH_PORT     ClickHouse HTTP port     (default: 8123)
    CH_DATABASE Database name            (default: default)
    CH_USER     Username                 (default: default)
    CH_PASSWORD Password                 (default: default)
"""

import os
from pathlib import Path

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

# Load .env from repo root if present
load_dotenv(Path(__file__).parent.parent / ".env", override=False)


def get_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "localhost"),
        port=int(os.getenv("CH_PORT", "8123")),
        database=os.getenv("CH_DATABASE", "default"),
        username=os.getenv("CH_USER", "default"),
        password=os.getenv("CH_PASSWORD", "default"),
    )


def query_df(sql: str, **kwargs) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame."""
    client = get_client()
    result = client.query(sql, **kwargs)
    return pd.DataFrame(result.result_rows, columns=result.column_names)
