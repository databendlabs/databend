SET enable_merge_into_row_fetch = 1;
SET enable_mutation_block_id_repartition = 1;

-- The MATCHED condition intentionally disables the update-column-only shortcut.
-- Source rows are sparse but distributed across the complete target id range.
MERGE INTO merge_cache_target AS target
USING merge_cache_source AS source
ON target.id = source.id
WHEN MATCHED AND target.version < source.version THEN UPDATE SET
    version = source.version,
    update_marker = source.update_marker;

SELECT count(), min(version), max(version), count_if(version = 2)
FROM merge_cache_target;
