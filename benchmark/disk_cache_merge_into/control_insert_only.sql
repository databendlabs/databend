-- Control query: no matched clause means no target RowFetch is required.
-- All generated ids are outside the target range.
MERGE INTO merge_cache_target AS target
USING (
    SELECT
        5000000 + number AS id,
        2::UINT32 AS version,
        concat('inserted-', md5(to_string(number))) AS update_marker,
        repeat('i', 128) AS payload_01,
        repeat('i', 128) AS payload_02,
        repeat('i', 128) AS payload_03,
        repeat('i', 128) AS payload_04,
        repeat('i', 128) AS payload_05,
        repeat('i', 128) AS payload_06,
        repeat('i', 128) AS payload_07,
        repeat('i', 128) AS payload_08,
        repeat('i', 128) AS payload_09,
        repeat('i', 128) AS payload_10,
        repeat('i', 128) AS payload_11,
        repeat('i', 128) AS payload_12,
        repeat('i', 128) AS payload_13,
        repeat('i', 128) AS payload_14,
        repeat('i', 128) AS payload_15,
        repeat('i', 128) AS payload_16
    FROM numbers(250000)
) AS source
ON target.id = source.id
WHEN NOT MATCHED THEN INSERT (
    id,
    group_id,
    version,
    update_marker,
    payload_01,
    payload_02,
    payload_03,
    payload_04,
    payload_05,
    payload_06,
    payload_07,
    payload_08,
    payload_09,
    payload_10,
    payload_11,
    payload_12,
    payload_13,
    payload_14,
    payload_15,
    payload_16
) VALUES (
    source.id,
    (source.id % 4096)::UINT32,
    source.version,
    source.update_marker,
    source.payload_01,
    source.payload_02,
    source.payload_03,
    source.payload_04,
    source.payload_05,
    source.payload_06,
    source.payload_07,
    source.payload_08,
    source.payload_09,
    source.payload_10,
    source.payload_11,
    source.payload_12,
    source.payload_13,
    source.payload_14,
    source.payload_15,
    source.payload_16
);
