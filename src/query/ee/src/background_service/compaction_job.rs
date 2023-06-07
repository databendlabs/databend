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

use std::fmt::format;
use std::sync::Arc;
use tracing::debug;
use arrow_array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use background_service::background_service::BackgroundServiceHandlerWrapper;
use common_config::InnerConfig;
use crate::background_service::configs::JobConfig;
use crate::background_service::job::Job;
use common_exception::Result;
use common_expression::types::{Float32Type, Float64Type};
use common_meta_app::background::CompactionStats;
use common_meta_app::schema::TableStatistics;
use crate::background_service::RealBackgroundService;

// TODO(zhihanz) add more configs to filter out tables need to be compacted
const GET_ALL_TARGET_TABLES: &str = "
SELECT t.database as database, d.database_id as database_id, t.name as table, t.table_id as table_id
FROM system.tables as t
JOIN system.databases as d
ON t.database = d.name
WHERE t.database != 'system'
    AND t.database != 'information_schema'
    AND t.engine = 'FUSE'
    AND t.num_rows > 0
    AND t.data_compressed_size > 0;
";

const SEGMENT_SIZE: u64 = 10;
const PER_SEGMENT_BLOCK: u64 = 100;
const PER_BLOCK_SIZE: u64 = 50; // MB

#[derive(Clone)]
pub struct CompactionJob {
    config: JobConfig,
    conf: InnerConfig,
}

#[async_trait::async_trait]
impl Job for CompactionJob {
    async fn run(&self) {
        self.do_compaction_job().await.expect("TODO: panic message");
    }

    fn get_config(&self) -> &JobConfig {
        &self.config
    }
}

//Service
// optimize table limit
// vacuum
impl CompactionJob {

    async fn do_compaction_job(&self) -> Result<()> {
        Ok(())
    }

    pub async fn do_get_all_target_tables(svc: &Arc<BackgroundServiceHandlerWrapper> ) -> Result<Option<RecordBatch>> {
        let res = svc.execute_sql(GET_ALL_TARGET_TABLES).await?;
        let num_of_tables = res.as_ref().map_or_else(|| 0, |r| r.num_rows());
        debug!(job="compaction", background=true, tables=num_of_tables, "get all target tables");
        Ok(res)
    }

    pub async fn do_check_table(svc: &Arc<BackgroundServiceHandlerWrapper> , database: String, table: String, seg_size: u64, avg_seg: u64, avg_blk: u64) -> Result<(bool, bool, TableStatistics)> {
        let sql = Self::get_compaction_advice_sql(database, table, seg_size, avg_seg, avg_blk);
        debug!(job="compaction", background=true, sql=sql.as_str(), "get all target tables");
        let res = svc.execute_sql(sql.as_str()).await?;
        if res.is_none() {
            return Ok((false, false, TableStatistics::default()));
        }
        let res = res.unwrap();
        let need_segment_compact = res.column(0).as_any().downcast_ref::<BooleanArray>().unwrap().value(0);
        let need_block_compact = res.column(1).as_any().downcast_ref::<BooleanArray>().unwrap().value(0);

        let table_statistics = TableStatistics {
            number_of_rows: res.column(2).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
            data_bytes: res.column(3).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
            compressed_data_bytes: res.column(4).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
            index_data_bytes: res.column(5).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
            number_of_segments: Some(res.column(6).as_any().downcast_ref::<UInt64Array>().unwrap().value(0)),
            number_of_blocks: Some(res.column(7).as_any().downcast_ref::<UInt64Array>().unwrap().value(0)),
        };

        Ok((need_segment_compact, need_block_compact, table_statistics))
    }

    async fn do_segment_compaction(&self, svc: &Arc<BackgroundServiceHandlerWrapper> , database: String, table: String) -> Result<()> {
        let sql = Self::get_segment_compaction_sql(database, table, self.conf.background.compaction.segment_limit);
        svc.execute_sql(sql.as_str()).await?;
        Ok(())
    }

    async fn do_block_compaction(&self, svc: &Arc<BackgroundServiceHandlerWrapper> , database: String, table: String) -> Result<()> {

        let sql = Self::get_block_compaction_sql(database, table, self.conf.background.compaction.block_limit);
        debug!(job="compaction", background=true, sql=sql.as_str(), "block_compaction");
        svc.execute_sql(sql.as_str()).await?;
        Ok(())
    }

    pub fn get_compaction_advice_sql(database: String, table: String, seg_size: u64, avg_seg: u64, avg_blk: u64) -> String {
        format!("
        select
        IF(segment_count > {} and block_count / segment_count < {}, TRUE, FALSE) AS segment_advice,
        IF(bytes_uncompressed / block_count / 1024 / 1024 < {}, TRUE, FALSE) AS block_advice,
        row_count, bytes_uncompressed, bytes_compressed, index_size,
        segment_count, block_count,
        block_count/segment_count,
        humanize_size(bytes_uncompressed / block_count) AS per_block_uncompressed_size_string
        from fuse_snapshot('{}', '{}') order by timestamp ASC LIMIT 1;
        ", seg_size, avg_seg, avg_blk, database, table)
    }
    pub fn get_segment_compaction_sql(database: String, table: String, limit: Option<u64>) -> String {
        let limit = if let Some(s) = limit {
            format!(" LIMIT {}", s)
        } else {
            "".to_string()
        };
        format!("OPTIMIZE TABLE {}.{} COMPACT SEGMENT{};", database, table, limit)
    }

    pub fn get_block_compaction_sql(database: String, table: String, limit: Option<u64>) -> String {
        let limit = if let Some(s) = limit {
            format!(" LIMIT {}", s)
        } else {
            "".to_string()
        };
        format!("OPTIMIZE TABLE {}.{} COMPACT{};", database, table, limit)
    }
}