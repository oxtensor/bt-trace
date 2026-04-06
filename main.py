import asyncio
import logging

from bittensor.core.async_subtensor import AsyncSubtensor

from db import DB_PATH, init_db, save_block_snapshots

NETWORK = "local"
logger = logging.getLogger("MAIN")


async def main():
    logger.info("Opening database at %s", DB_PATH)
    conn = init_db()
    try:
        logger.info("Connecting to subtensor (network=%s)", NETWORK)
        async with AsyncSubtensor(network=NETWORK) as subtensor:
            logger.info("Subtensor ready; waiting for blocks")
            while True:
                await subtensor.wait_for_block()
                block = await subtensor.get_current_block()
                subnets = await subtensor.all_subnets(block=block)
                if subnets is None:
                    logger.warning("all_subnets returned None at block %s; skipping", block)
                    continue
                save_block_snapshots(conn, block, subnets)
                logger.info("Block %s: saved %s subnet row(s)", block, len(subnets))
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user (Ctrl+C)")
