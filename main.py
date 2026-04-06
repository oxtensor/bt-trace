import asyncio
import os
import time
from loguru import logger
from dotenv import load_dotenv
from bittensor.core.async_subtensor import AsyncSubtensor

from db import DB_PATH, init_db, save_block_snapshots

load_dotenv()
NETWORK = os.getenv("NETWORK", "finney")


async def main():
    logger.info("Opening database at {}", DB_PATH)
    conn = init_db()
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
                    logger.info("Block {}: saved {} subnet row(s)", block, len(subnets))
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user (Ctrl+C)")
