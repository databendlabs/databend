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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableStatistics;
use databend_enterprise_query::background_service::should_continue_compaction;
use databend_enterprise_query::background_service::CompactionJob;

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
        "select\n        IF(block_count > 10 and block_count / segment_count < 100, TRUE, FALSE) AS segment_advice,\n        IF(row_count > 100000 AND block_count > 10 AND bytes_uncompressed / block_count / 1024 / 1024  < 50, TRUE, FALSE) AS block_advice,\n        row_count, bytes_uncompressed, bytes_compressed, index_size,\n        segment_count, block_count,\n        block_count/segment_count\n        from fuse_snapshot('db1', 'tbl1') order by timestamp ASC LIMIT 1;",
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
async fn test_parse_target_tables() -> Result<()> {
    let tables = CompactionJob::parse_all_target_tables(Some(&vec![
        "db1.table1".to_string(),
        "db1.table2".to_string(),
    ]));
    assert_eq!(tables.len(), 1);
    assert_eq!(tables.get("db1").unwrap().len(), 2);
    assert_eq!(tables.get("db1").unwrap().first().unwrap(), "table1");
    assert_eq!(tables.get("db1").unwrap().get(1).unwrap(), "table2");
    let tables = CompactionJob::parse_all_target_tables(Some(&vec![
        "db1.table1".to_string(),
        "tb2".to_string(),
    ]));
    assert_eq!(tables.len(), 2);
    assert_eq!(tables.get("db1").unwrap().len(), 1);
    assert_eq!(tables.get("db1").unwrap().first().unwrap(), "table1");
    assert_eq!(tables.get("default").unwrap().first().unwrap(), "tb2");
    let tables = CompactionJob::parse_all_target_tables(Some(&vec![
        "iceberg.db1.table1".to_string(),
        "tb2".to_string(),
    ]));
    assert_eq!(tables.len(), 1);

    assert_eq!(tables.get("default").unwrap().first().unwrap(), "tb2");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_target_table_from_configs() -> Result<()> {
    let sql = CompactionJob::get_target_from_config_sql("db1".to_string(), vec![
        "table1".to_string(),
        "table2".to_string(),
    ]);
    assert_eq!(
        sql.trim(),
        "SELECT t.database as database, d.database_id as database_id, t.name as table, t.table_id as table_id\n        FROM system.tables as t\n        JOIN system.databases as d\n        ON t.database = d.name\n        WHERE t.database != 'system'\n            AND t.database != 'information_schema'\n            AND t.engine = 'FUSE'\n            AND t.database = 'db1'\n            AND t.name IN ('table1', 'table2')\n            ;"
    );
    let sql =
        CompactionJob::get_target_from_config_sql("db1".to_string(), vec!["table1".to_string()]);
    assert_eq!(
        sql.trim(),
        "SELECT t.database as database, d.database_id as database_id, t.name as table, t.table_id as table_id\n        FROM system.tables as t\n        JOIN system.databases as d\n        ON t.database = d.name\n        WHERE t.database != 'system'\n            AND t.database != 'information_schema'\n            AND t.engine = 'FUSE'\n            AND t.database = 'db1'\n            AND t.name IN ('table1')\n            ;"
    );

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
        number_of_segments: Some(1),
        ..Default::default()
    };
    assert_eq!(should_continue_compaction(&old, &new), (false, false));
    let old = TableStatistics {
        number_of_blocks: Some(1002),
        number_of_segments: Some(100),
        ..Default::default()
    };
    let new = TableStatistics {
        number_of_blocks: Some(1001),
        number_of_segments: Some(90),
        ..Default::default()
    };
    assert_eq!(should_continue_compaction(&old, &new), (true, false));
    let old = TableStatistics {
        number_of_blocks: Some(10000),
        number_of_segments: Some(900),
        data_bytes: 50 * 50 * 1024 * 1024,
        ..Default::default()
    };
    let new = TableStatistics {
        number_of_blocks: Some(9000),
        number_of_segments: Some(900),
        data_bytes: 50 * 50 * 1024 * 1024,
        ..Default::default()
    };
    assert_eq!(should_continue_compaction(&old, &new), (true, true));
    let old = TableStatistics {
        number_of_blocks: Some(10000),
        number_of_segments: Some(10),
        data_bytes: 50 * 1001 * 1024 * 1024,
        ..Default::default()
    };
    let new = TableStatistics {
        number_of_blocks: Some(9010),
        number_of_segments: Some(9),
        data_bytes: 50 * 100 * 1001 * 1024 * 1024,
        ..Default::default()
    };
    assert_eq!(should_continue_compaction(&old, &new), (false, false));
    Ok(())
}
