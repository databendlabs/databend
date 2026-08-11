-- Default scale:
--   target rows: 5,000,000
--   source rows:   250,000 (5%, distributed across the whole target)
--   payload: 16 STRING columns, 128 bytes per column before compression
--
-- For a quick smoke test, replace numbers(5000000) with numbers(1000000)
-- and replace 5000000 in source generation with 1000000.

DROP TABLE IF EXISTS merge_cache_target;
DROP TABLE IF EXISTS merge_cache_source;
DROP TABLE IF EXISTS merge_cache_seed;

CREATE TABLE merge_cache_seed (
    id UINT64,
    group_id UINT32,
    version UINT32,
    update_marker STRING,
    payload_01 STRING,
    payload_02 STRING,
    payload_03 STRING,
    payload_04 STRING,
    payload_05 STRING,
    payload_06 STRING,
    payload_07 STRING,
    payload_08 STRING,
    payload_09 STRING,
    payload_10 STRING,
    payload_11 STRING,
    payload_12 STRING,
    payload_13 STRING,
    payload_14 STRING,
    payload_15 STRING,
    payload_16 STRING
);

INSERT INTO merge_cache_seed
WITH generated AS (
    SELECT
        number AS id,
        md5(concat('a-', to_string(number))) AS h1,
        md5(concat('b-', to_string(number))) AS h2,
        md5(concat('c-', to_string(number))) AS h3,
        md5(concat('d-', to_string(number))) AS h4
    FROM numbers(5000000)
)
SELECT
    id,
    (id % 4096)::UINT32 AS group_id,
    1::UINT32 AS version,
    concat('seed-', to_string(id)) AS update_marker,
    concat(h1, h2, h3, h4),
    concat(h2, h3, h4, h1),
    concat(h3, h4, h1, h2),
    concat(h4, h1, h2, h3),
    concat(h1, h3, h2, h4),
    concat(h1, h4, h2, h3),
    concat(h2, h1, h4, h3),
    concat(h2, h4, h1, h3),
    concat(h3, h1, h4, h2),
    concat(h3, h2, h4, h1),
    concat(h4, h2, h1, h3),
    concat(h4, h3, h2, h1),
    concat(h1, h2, h4, h3),
    concat(h2, h1, h3, h4),
    concat(h3, h4, h2, h1),
    concat(h4, h3, h1, h2)
FROM generated;

CREATE TABLE merge_cache_source (
    id UINT64,
    version UINT32,
    update_marker STRING
);

-- 1,000,003 is coprime with 5,000,000. It permutes source ids across the
-- complete target range so sparse updates touch as many target blocks as possible.
INSERT INTO merge_cache_source
SELECT
    (number * 1000003) % 5000000 AS id,
    2::UINT32 AS version,
    concat('updated-', md5(to_string(number))) AS update_marker
FROM numbers(250000);

ANALYZE TABLE merge_cache_seed;
ANALYZE TABLE merge_cache_source;

CREATE TABLE merge_cache_target LIKE merge_cache_seed;
INSERT INTO merge_cache_target SELECT * FROM merge_cache_seed;
ANALYZE TABLE merge_cache_target;

SELECT
    name,
    num_rows,
    data_size,
    data_compressed_size,
    number_of_blocks
FROM system.tables
WHERE database = current_database()
  AND name IN ('merge_cache_seed', 'merge_cache_source', 'merge_cache_target')
ORDER BY name;
