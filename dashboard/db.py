import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

DB_PATH = Path(__file__).parent / "alerts.db"


def load_alerts(limit: int = 5000) -> pd.DataFrame:
    """Return the most recent alerts as a DataFrame (details parsed to dict)."""
    if not DB_PATH.exists():
        return pd.DataFrame(columns=["rule", "user_id", "alert_ts", "details"])
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query(
        "SELECT rule, user_id, alert_ts, details "
        "FROM alerts ORDER BY id DESC LIMIT ?",
        conn, params=(limit,),
    )
    conn.close()
    if df.empty:
        return pd.DataFrame(columns=["rule", "user_id", "alert_ts", "details"])
    df["details"] = df["details"].apply(_safe_json)
    return df


def db_count() -> int:
    if not DB_PATH.exists():
        return 0
    conn = sqlite3.connect(str(DB_PATH))
    cnt = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    conn.close()
    return cnt


def _safe_json(s: Any) -> dict[str, Any]:
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return {}
