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

use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::UInt64Array;
use chrono::Utc;
use background_service::background_service::BackgroundServiceHandlerWrapper;
use common_config::{CompactionParams, InnerConfig};
use common_exception::Result;
use common_expression::types::Float32Type;
use common_expression::types::Float64Type;
use common_meta_app::background::{BackgroundJobIdent, BackgroundJobInfo, BackgroundJobType, BackgroundTaskIdent, BackgroundTaskInfo, CompactionStats, UpdateBackgroundTaskReq};
use common_meta_app::schema::TableStatistics;
use tracing::{debug, info};
use common_base::base::uuid::Uuid;
use common_meta_api::BackgroundApi;
use common_meta_store::MetaStore;
use common_users::UserApiProvider;

use crate::background_service::configs::JobConfig;
use crate::background_service::job::Job;
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

const EXPIRE_SEC: u64 = 60 * 60 * 24 * 7; // 7 days

#[derive(Clone)]
pub struct CompactionJob {
    conf: InnerConfig,
    meta_api: Arc<MetaStore>,
    creator: BackgroundJobIdent,
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

// continue to compact
fn should_continue_compaction(old: &TableStatistics, new: &TableStatistics) -> (bool, bool) {
    if old.number_of_blocks.is_none() || old.number_of_segments.is_none() ||
        new.number_of_blocks.is_none() || new.number_of_segments.is_none() {
        return (false, false);
    }
    let old_segment_density = old.number_of_blocks.unwrap() as f64 / old.number_of_segments.unwrap() as f64;
    let new_segment_density = new.number_of_blocks.unwrap() as f64 / new.number_of_segments.unwrap() as f64;
    let should_continue_seg_compact = new_segment_density > old_segment_density;
    let old_block_density = old.data_bytes.unwrap() as f64 / old.number_of_blocks.unwrap() as f64;
    let new_block_density = new.data_bytes.unwrap() as f64 / new.number_of_blocks.unwrap() as f64;
    let should_continue_blk_compact = new_block_density > old_block_density;
    (should_continue_seg_compact, should_continue_blk_compact)
}

// Service
// optimize table limit
// vacuum
impl CompactionJob {
    pub async fn create(&self, config: &InnerConfig, name: String) -> Self{
        let tenant = config.query.tenant_id.clone();
        let creator = BackgroundJobIdent{ tenant, name };
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        Self {
            conf: config.clone(),
            meta_api,
            creator,
        }
    }
    async fn do_compaction_job(&self) -> Result<()> {
        Ok(())
    }


    // continuous compact on table until it's not needed
    async fn compact_table(&self, svc: &Arc<BackgroundServiceHandlerWrapper>,
                                         database: String,
                                         table: String) -> Result<bool> {
        let (seg, blk, stats) = Self::do_check_table(svc, database.clone(), table.clone(), SEGMENT_SIZE, PER_SEGMENT_BLOCK, PER_BLOCK_SIZE).await?;
        if !seg && !blk {
            return Ok(false);
        }

        let mut old = stats;
        if seg {
            loop {
                self.do_segment_compaction(svc, database.clone(), table.clone()).await?;
                let (_, _, new) = Self::do_check_table(svc, database.clone(), table.clone(), SEGMENT_SIZE, PER_SEGMENT_BLOCK, PER_BLOCK_SIZE).await?;
                if !should_continue_compaction(&old, &new).0 {
                    old = new;
                    break;
                }
                old = new;
            }
        }

        if blk {
            loop {
                self.do_block_compaction(svc, database.clone(), table.clone()).await?;
                let (_, _, new) = Self::do_check_table(svc, database.clone(), table.clone(), SEGMENT_SIZE, PER_SEGMENT_BLOCK, PER_BLOCK_SIZE).await?;
                if !should_continue_compaction(&old, &new).1 {
                    old = new;
                    break;
                }
                old = new;
            }
        }

        Ok(true)
    }

    pub async fn do_get_all_target_tables(
        svc: &Arc<BackgroundServiceHandlerWrapper>,
    ) -> Result<Option<RecordBatch>> {
        let res = svc.execute_sql(GET_ALL_TARGET_TABLES).await?;
        let num_of_tables = res.as_ref().map_or_else(|| 0, |r| r.num_rows());
        info!(
            job = "compaction",
            background = true,
            tables = num_of_tables,
            "get all target tables"
        );
        Ok(res)
    }

    pub async fn do_check_table(
        svc: &Arc<BackgroundServiceHandlerWrapper>,
        database: String,
        table: String,
        seg_size: u64,
        avg_seg: u64,
        avg_blk: u64,
    ) -> Result<(bool, bool, TableStatistics)> {
        let sql = Self::get_compaction_advice_sql(database, table, seg_size, avg_seg, avg_blk);
        debug!(
            job = "compaction",
            background = true,
            sql = sql.as_str(),
            "check target_table"
        );
        let res = svc.execute_sql(sql.as_str()).await?;
        if res.is_none() {
            return Ok((false, false, TableStatistics::default()));
        }
        let res = res.unwrap();
        let need_segment_compact = res
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0);
        let need_block_compact = res
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0);

        let table_statistics = TableStatistics {
            number_of_rows: res
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            data_bytes: res
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            compressed_data_bytes: res
                .column(4)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            index_data_bytes: res
                .column(5)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            number_of_segments: Some(
                res.column(6)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
            ),
            number_of_blocks: Some(
                res.column(7)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
            ),
        };

        Ok((need_segment_compact, need_block_compact, table_statistics))
    }

    async fn do_segment_compaction(
        &self,
        svc: &Arc<BackgroundServiceHandlerWrapper>,
        database: String,
        table: String,
    ) -> Result<()> {
        let sql = Self::get_segment_compaction_sql(
            database,
            table,
            self.conf.background.compaction.segment_limit,
        );
        svc.execute_sql(sql.as_str()).await?;
        Ok(())
    }

    async fn do_block_compaction(
        &self,
        svc: &Arc<BackgroundServiceHandlerWrapper>,
        database: String,
        table: String,
    ) -> Result<()> {
        let sql = Self::get_block_compaction_sql(
            database,
            table,
            self.conf.background.compaction.block_limit,
        );
        debug!(
            job = "compaction",
            background = true,
            sql = sql.as_str(),
            "block_compaction"
        );
        svc.execute_sql(sql.as_str()).await?;
        Ok(())
    }

    pub fn get_compaction_advice_sql(
        database: String,
        table: String,
        seg_size: u64,
        avg_seg: u64,
        avg_blk: u64,
    ) -> String {
        format!(
            "
        select
        IF(segment_count > {} and block_count / segment_count < {}, TRUE, FALSE) AS segment_advice,
        IF(bytes_uncompressed / block_count / 1024 / 1024 < {}, TRUE, FALSE) AS block_advice,
        row_count, bytes_uncompressed, bytes_compressed, index_size,
        segment_count, block_count,
        block_count/segment_count,
        humanize_size(bytes_uncompressed / block_count) AS per_block_uncompressed_size_string
        from fuse_snapshot('{}', '{}') order by timestamp ASC LIMIT 1;
        ",
            seg_size, avg_seg, avg_blk, database, table
        )
    }
    pub fn get_segment_compaction_sql(
        database: String,
        table: String,
        limit: Option<u64>,
    ) -> String {
        let limit = if let Some(s) = limit {
            format!(" LIMIT {}", s)
        } else {
            "".to_string()
        };
        format!(
            "OPTIMIZE TABLE {}.{} COMPACT SEGMENT{};",
            database, table, limit
        )
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
