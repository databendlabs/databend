-- Read all payload columns so the raw column cache is populated before MERGE.
-- ignore_result avoids transferring a result set while still forcing the scan.
SELECT
    sum(
        id + group_id + version + length(update_marker) +
        length(payload_01) + length(payload_02) +
        length(payload_03) + length(payload_04) +
        length(payload_05) + length(payload_06) +
        length(payload_07) + length(payload_08) +
        length(payload_09) + length(payload_10) +
        length(payload_11) + length(payload_12) +
        length(payload_13) + length(payload_14) +
        length(payload_15) + length(payload_16)
    )
FROM merge_cache_target
IGNORE_RESULT;

SELECT
    node,
    name,
    num_items,
    size,
    capacity,
    access,
    hit,
    miss
FROM system.caches
WHERE name IN ('disk_cache_column_data', 'memory_cache_column_data')
ORDER BY node, name;
