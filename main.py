import asyncio
import os
import time
from loguru import logger
from dotenv import load_dotenv
from async_substrate_interface import AsyncSubstrateInterface
from scalecodec.utils.ss58 import ss58_encode
from bittensor.core.async_subtensor import AsyncSubtensor

from db import DB_PATH, init_db, save_alpha_trades, save_block_snapshots

load_dotenv()
NETWORK = os.getenv("NETWORK", "finney")
WS_ENDPOINT = os.getenv("WS_ENDPOINT", "wss://entrypoint-finney.opentensor.ai:443")

STAKE_ADDED = "SubtensorModule.StakeAdded"
STAKE_REMOVED = "SubtensorModule.StakeRemoved"
TRADE_SIDE_MAP = {
    STAKE_ADDED: "buy",
    STAKE_REMOVED: "sell",
}
SS58_FORMAT = 42


def unwrap_scale_payload(item):
    if isinstance(item, dict):
        return item.get("value", item)
    return getattr(item, "value", item)


def pub2ss58(pub_hex: str) -> str | None:
    if not pub_hex:
        return None
    try:
        if not pub_hex.startswith("0x"):
            pub_hex = "0x" + pub_hex
        pub_bytes = bytes.fromhex(pub_hex[2:])
        return ss58_encode(pub_bytes, SS58_FORMAT)
    except Exception:
        print(f"Error converting pub key to SS58: {pub_hex}")
        return None


def parse_stake_event_attributes(event_attributes: list) -> dict:
    sender_pub, hotkey_pub, tao_amount, alpha_amount, netuid, fee = event_attributes
    sender_ss58 = pub2ss58(sender_pub) or str(sender_pub)
    hotkey_ss58 = pub2ss58(hotkey_pub) or str(hotkey_pub)
    return {
        "sender_ss58": sender_ss58,
        "hotkey_ss58": hotkey_ss58,
        "tao_amount_rao": int(tao_amount),
        "alpha_amount_rao": int(alpha_amount),
        "netuid": int(netuid),
        "fee_rao": int(fee),
    }


async def extract_alpha_trades(substrate: AsyncSubstrateInterface, block_num: int) -> list[dict]:
    block_hash = await substrate.get_block_hash(block_num)
    extrinsics = await substrate.get_extrinsics(block_hash=block_hash)
    tx_method_by_idx = {}
    for idx, extrinsic in enumerate(extrinsics):
        ex_val = unwrap_scale_payload(extrinsic)
        call = ex_val["call"]
        tx_method_by_idx[idx] = f"{call['call_module']}.{call['call_function']}"

    events = await substrate.get_events(block_hash=block_hash)
    trades: list[dict] = []
    for event_idx, event in enumerate(events):
        ev = unwrap_scale_payload(event)
        event_method = f"{ev['event']['module_id']}.{ev['event']['event_id']}"
        if event_method not in TRADE_SIDE_MAP:
            continue

        parsed = parse_stake_event_attributes(ev["event"]["attributes"])
        ex_idx = ev.get("extrinsic_idx")
        tx_method = tx_method_by_idx.get(ex_idx) if ex_idx is not None else None
        alpha_amount = parsed["alpha_amount_rao"]
        price = parsed["tao_amount_rao"] / alpha_amount if alpha_amount > 0 else None
        trades.append(
            {
                "block": block_num,
                "extrinsic_idx": ex_idx,
                "event_idx": event_idx,
                "event_method": event_method,
                "side": TRADE_SIDE_MAP[event_method],
                "tx_method": tx_method,
                **parsed,
                "price_tao_per_alpha": price,
            }
        )
    return trades


async def main():
    logger.info("Opening database at {}", DB_PATH)
    conn = init_db()
    logger.info("Connecting event parser substrate endpoint ({})", WS_ENDPOINT)
    substrate = AsyncSubstrateInterface(url=WS_ENDPOINT, use_remote_preset=True)
    try:
        logger.info("Connecting to subtensor (network={})", NETWORK)
        async with AsyncSubtensor(network=NETWORK) as subtensor:
            logger.info("Subtensor ready; waiting for blocks")
            while True:
                await subtensor.wait_for_block()
                block = await subtensor.get_current_block()
                t0 = time.perf_counter()
                subnets = await subtensor.all_subnets(block=block)
                t1 = time.perf_counter()
                logger.info("all_subnets took {} seconds", t1 - t0)
                if subnets is not None:
                    save_block_snapshots(conn, block, subnets)
                    trades = await extract_alpha_trades(substrate, block)
                    trade_count = save_alpha_trades(conn, trades)
                    logger.info("Block {}: saved {} subnet row(s), {} trade row(s)", block, len(subnets), trade_count)
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user (Ctrl+C)")
