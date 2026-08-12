-- Recreate the mutable target before every measured MERGE.
DROP TABLE IF EXISTS merge_cache_target;
CREATE TABLE merge_cache_target LIKE merge_cache_seed;
INSERT INTO merge_cache_target SELECT * FROM merge_cache_seed;
ANALYZE TABLE merge_cache_target;

SELECT count(), min(version), max(version) FROM merge_cache_target;
