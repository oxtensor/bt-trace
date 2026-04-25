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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alpha_trade_history (
            block INTEGER NOT NULL,
            extrinsic_idx INTEGER,
            event_idx INTEGER NOT NULL,
            event_method TEXT NOT NULL,
            side TEXT NOT NULL,
            tx_method TEXT,
            netuid INTEGER NOT NULL,
            sender_ss58 TEXT NOT NULL,
            hotkey_ss58 TEXT NOT NULL,
            tao_amount_rao INTEGER NOT NULL,
            alpha_amount_rao INTEGER NOT NULL,
            fee_rao INTEGER NOT NULL,
            price_tao_per_alpha REAL,
            PRIMARY KEY (block, event_idx)
        )
    """)
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


def save_alpha_trades(conn: sqlite3.Connection, trades: list[dict]) -> int:
    if not trades:
        return 0

    rows = [
        (
            int(t["block"]),
            t["extrinsic_idx"],
            int(t["event_idx"]),
            str(t["event_method"]),
            str(t["side"]),
            t.get("tx_method"),
            int(t["netuid"]),
            str(t["sender_ss58"]),
            str(t["hotkey_ss58"]),
            int(t["tao_amount_rao"]),
            int(t["alpha_amount_rao"]),
            int(t["fee_rao"]),
            float(t["price_tao_per_alpha"]) if t["price_tao_per_alpha"] is not None else None,
        )
        for t in trades
    ]
    conn.executemany(
        """
        INSERT OR REPLACE INTO alpha_trade_history
        (
            block, extrinsic_idx, event_idx, event_method, side, tx_method,
            netuid, sender_ss58, hotkey_ss58, tao_amount_rao, alpha_amount_rao, fee_rao, price_tao_per_alpha
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)
