SET enable_merge_into_row_fetch = 1;
SET enable_mutation_block_id_repartition = 1;

EXPLAIN
MERGE INTO merge_cache_target AS target
USING merge_cache_source AS source
ON target.id = source.id
WHEN MATCHED AND target.version < source.version THEN UPDATE SET
    version = source.version,
    update_marker = source.update_marker;
