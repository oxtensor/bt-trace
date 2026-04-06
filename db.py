import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "snapshots.db"


def init_db(path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS subnet_snapshots (
            block INTEGER NOT NULL,
            netuid INTEGER NOT NULL,
            alpha_in_rao INTEGER NOT NULL,
            alpha_out_rao INTEGER NOT NULL,
            tao_in_rao INTEGER NOT NULL,
            price_rao INTEGER,
            moving_price REAL NOT NULL,
            PRIMARY KEY (block, netuid)
        )
    """)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    return conn


def save_block_snapshots(conn: sqlite3.Connection, block: int, subnets: list) -> None:
    rows = [
        (
            block,
            s.netuid,
            int(s.alpha_in.rao),
            int(s.alpha_out.rao),
            int(s.tao_in.rao),
            int(s.price.rao) if s.price is not None else None,
            float(s.moving_price),
        )
        for s in subnets
    ]
    if not rows:
        return
    conn.executemany(
        """
        INSERT OR REPLACE INTO subnet_snapshots
        (block, netuid, alpha_in_rao, alpha_out_rao, tao_in_rao, price_rao, moving_price)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
