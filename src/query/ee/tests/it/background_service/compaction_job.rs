// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::default::Default;

use common_base::base::tokio;
use common_exception::Result;
use common_meta_app::schema::TableStatistics;
use enterprise_query::background_service::should_continue_compaction;
use enterprise_query::background_service::CompactionJob;

#[tokio::test(flavor = "multi_thread")]
async fn test_get_compaction_advice_sql() -> Result<()> {
    let sql = CompactionJob::get_compaction_advice_sql(
        "db1".to_string(),
        "tbl1".to_string(),
        10,
        100,
        50,
    );
    assert_eq!(
        sql.trim(),
        "select\n        IF(segment_count > 10 and block_count / segment_count < 100, TRUE, FALSE) AS segment_advice,\n        IF(bytes_uncompressed / block_count / 1024 / 1024 < 50, TRUE, FALSE) AS block_advice,\n        row_count, bytes_uncompressed, bytes_compressed, index_size,\n        segment_count, block_count,\n        block_count/segment_count,\n        humanize_size(bytes_uncompressed / block_count) AS per_block_uncompressed_size_string\n        from fuse_snapshot('db1', 'tbl1') order by timestamp ASC LIMIT 1;"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_segment_compaction_sql() -> Result<()> {
    let sql =
        CompactionJob::get_segment_compaction_sql("db1".to_string(), "tbl1".to_string(), None);
    assert_eq!(sql.trim(), "OPTIMIZE TABLE db1.tbl1 COMPACT SEGMENT;");
    let sql = CompactionJob::get_segment_compaction_sql(
        "db1".to_string(),
        "tbl1".to_string(),
        Option::Some(100),
    );
    assert_eq!(
        sql.trim(),
        "OPTIMIZE TABLE db1.tbl1 COMPACT SEGMENT LIMIT 100;"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_block_compaction_sql() -> Result<()> {
    let sql = CompactionJob::get_block_compaction_sql("db1".to_string(), "tbl1".to_string(), None);
    assert_eq!(sql.trim(), "OPTIMIZE TABLE db1.tbl1 COMPACT;");
    let sql = CompactionJob::get_block_compaction_sql(
        "db1".to_string(),
        "tbl1".to_string(),
        Option::Some(100),
    );
    assert_eq!(sql.trim(), "OPTIMIZE TABLE db1.tbl1 COMPACT LIMIT 100;");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_should_continue_compaction() -> Result<()> {
    let old = TableStatistics {
        number_of_blocks: None,
        ..Default::default()
    };
    let new = TableStatistics {
        number_of_blocks: Some(100),
        ..Default::default()
    };
    assert_eq!(should_continue_compaction(&old, &new), (false, false));
    let old = TableStatistics {
        number_of_blocks: Some(100),
        number_of_segments: Some(10),
        ..Default::default()
    };
    let new = TableStatistics {
        number_of_blocks: Some(100),
        number_of_segments: Some(9),
        ..Default::default()
    };
    assert_eq!(should_continue_compaction(&old, &new), (true, false));
    let old = TableStatistics {
        number_of_blocks: Some(100),
        number_of_segments: Some(10),
        data_bytes: 100,
        ..Default::default()
    };
    let new = TableStatistics {
        number_of_blocks: Some(90),
        number_of_segments: Some(9),
        data_bytes: 100,
        ..Default::default()
    };
    assert_eq!(should_continue_compaction(&old, &new), (true, true));
    Ok(())
}
