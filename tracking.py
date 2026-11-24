def blocks_to_process(last_processed: int | None, current: int, max_backfill: int) -> tuple[list[int], int]:
    """Return the blocks whose trades still need extracting, and how many were dropped.

    ``wait_for_block`` only reports the newest block, so any block that arrived
    while the previous iteration was still working would otherwise be skipped.
    Every block after ``last_processed`` up to ``current`` is returned, capped
    at the newest ``max_backfill`` blocks so a long outage cannot ask a pruning
    node for state it no longer holds. The second value counts blocks dropped
    by that cap.
    """
    if last_processed is None:
        return [current], 0
    if current <= last_processed:
        return [], 0
    start = last_processed + 1
    dropped = 0
    if max_backfill > 0 and current - start + 1 > max_backfill:
        dropped = current - max_backfill + 1 - start
        start = current - max_backfill + 1
    return list(range(start, current + 1)), dropped
