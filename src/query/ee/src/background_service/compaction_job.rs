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

use std::sync::{Arc};
use std::thread::sleep;
use std::time::Duration;

use arrow_array::{BooleanArray, LargeBinaryArray};
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::UInt64Array;
use chrono::Utc;
use futures_util::SinkExt;
use serde::de::Unexpected::Str;
use background_service::background_service::BackgroundServiceHandlerWrapper;
use common_config::{InnerConfig};
use common_exception::Result;
use common_meta_app::background::{BackgroundJobIdent, BackgroundTaskIdent, BackgroundTaskInfo, BackgroundTaskState, UpdateBackgroundTaskReq};
use common_meta_app::schema::TableStatistics;
use tracing::{debug, error, info};
use background_service::get_background_service_handler;
use common_base::base::tokio::time::Instant;
use common_base::base::uuid::{Uuid};
use common_meta_api::BackgroundApi;
use common_meta_store::MetaStore;
use common_users::UserApiProvider;
use common_base::base::tokio::sync::{futures, Mutex, RwLock};
use common_base::base::tokio::sync::mpsc::Sender;

use crate::background_service::job::Job;

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

const SEGMENT_SIZE: u64 = 0;
const PER_SEGMENT_BLOCK: u64 = 1;
const PER_BLOCK_SIZE: u64 = 1; // MB

const EXPIRE_SEC: u64 = 60 * 60 * 24 * 7; // 7 days

#[derive(Clone)]
pub struct CompactionJob {
    conf: InnerConfig,
    meta_api: Arc<MetaStore>,
    creator: BackgroundJobIdent,
    finish_tx: Arc<Mutex<Sender<u64>>>,
}

#[async_trait::async_trait]
impl Job for CompactionJob {
    async fn run(&self) {
        self.do_compaction_job().await.expect("TODO: panic message");
    }
}

// continue to compact
pub fn should_continue_compaction(old: &TableStatistics, new: &TableStatistics) -> (bool, bool) {
    if old.number_of_blocks.is_none() || old.number_of_segments.is_none() ||
        new.number_of_blocks.is_none() || new.number_of_segments.is_none() {
        return (false, false);
    }
    let old_segment_density = old.number_of_blocks.unwrap() as f64 / old.number_of_segments.unwrap() as f64;
    let new_segment_density = new.number_of_blocks.unwrap() as f64 / new.number_of_segments.unwrap() as f64;
    let should_continue_seg_compact = new_segment_density > old_segment_density || old.number_of_blocks != new.number_of_blocks || old.number_of_segments != new.number_of_segments;
    let old_block_density = old.data_bytes as f64 / old.number_of_blocks.unwrap() as f64;
    let new_block_density = new.data_bytes as f64 / new.number_of_blocks.unwrap() as f64;
    let should_continue_blk_compact = new_block_density > old_block_density || old.data_bytes != new.data_bytes || old.number_of_blocks != new.number_of_blocks;
    (should_continue_seg_compact, should_continue_blk_compact)
}

// Service
// optimize table limit
// vacuum
impl CompactionJob {
    pub async fn create(config: &InnerConfig, name: String, finish_tx: Arc<Mutex<Sender<u64>>>) -> Self{
        let tenant = config.query.tenant_id.clone();
        let creator = BackgroundJobIdent{ tenant, name };
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        Self {
            conf: config.clone(),
            meta_api,
            creator,
            finish_tx
        }
    }
    async fn do_compaction_job(&self) -> Result<()>{
        let svc = get_background_service_handler();
        if let Some(records) = Self::do_get_all_target_tables(&svc).await? {
            debug!(?records, "target_tables");
            let db_names = records.column(0).as_any().downcast_ref::<arrow_array::LargeBinaryArray>().unwrap();
            let db_ids = records.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
            let tb_names = records.column(2).as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let tb_ids = records.column(3).as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..records.num_rows() {
                let db_name = String::from_utf8_lossy(db_names.value(i)).to_string();
                let db_id = db_ids.value(i);
                let tb_name = String::from_utf8_lossy(tb_names.value(i)).to_string();
                let tb_id = tb_ids.value(i);
                match self.compact_table(&svc, db_name.clone(), tb_name.clone(), db_id, tb_id).await {
                    Ok(_) => {
                        info!("compaction job success, db: {}, table: {}", db_name, tb_name);
                    }
                    Err(e) => {
                        error!("compaction job failed, db: {}, table: {}, err: {}", db_name, tb_name, e);
                    }
                }
            }
        }
        info!(job = "compaction", background = true, "compaction job done");
        let mut finish_tx = self.finish_tx.clone();
        let _ = finish_tx.lock().await.send(1).await;
        Ok(())

    }

    fn set_task_status(info: &mut BackgroundTaskInfo, state: BackgroundTaskState) {
        info.task_state = state;
        info.last_updated = Some(Utc::now());
    }

    fn set_task_stats(info: &mut BackgroundTaskInfo, updated: TableStatistics, time: Duration) {
        if let Some(mut stats) = info.compaction_task_stats.clone() {
            stats.after_compaction_stats = Some(updated);
            stats.total_compaction_time = Some(time);
            info.compaction_task_stats = Some(stats);
            info.last_updated = Some(Utc::now());
        }
    }

    async fn compact_table(&self, svc: &Arc<BackgroundServiceHandlerWrapper>, database: String, table: String, db_id: u64, tb_id: u64) -> Result<()> {

        let (seg, blk, stats) = Self::do_check_table(svc, database.clone(), table.clone(), SEGMENT_SIZE, PER_SEGMENT_BLOCK, PER_BLOCK_SIZE).await?;
        if !seg && !blk {
            info!(job = "compaction", background = true, database = database.clone(), table = table.clone(), should_compact_segment = seg, should_compact_blk = blk, table_stats = ?stats, "skip compact");
            return Ok(());
        }
        let id = Uuid::new_v4().to_string();
        info!(job = "compaction", background = true, id=id.clone(), database = database.clone(), table = table.clone(), should_compact_segment = seg, should_compact_blk = blk, table_stats = ?stats, "start compact");
        let task_name = BackgroundTaskIdent{
            tenant: self.creator.tenant.clone(),
            task_id: id.clone(),
        };

        let mut info = BackgroundTaskInfo::new_compaction_task(self.creator.clone(), db_id, tb_id, stats, format!("need segment compaction: {}, need block compaction: {}", seg, blk));
        self.meta_api.update_background_task(UpdateBackgroundTaskReq{
            task_name: task_name.clone(),
            task_info: info.clone(),
            expire_at: Utc::now().timestamp() as  u64 + EXPIRE_SEC,
        }).await?;

        let start = Instant::now();
        match self.do_compact_table(svc, database.clone(), table.clone()).await {
            Ok(_) => {
                let (_, _, new_stats) = Self::do_check_table(svc, database.clone(), table.clone(), SEGMENT_SIZE, PER_SEGMENT_BLOCK, PER_BLOCK_SIZE).await?;
                Self::set_task_stats(&mut info, new_stats.clone(), start.elapsed());
                Self::set_task_status(&mut info, BackgroundTaskState::DONE);
                info!(job = "compaction", background = true, id=id.clone(), database = database.clone(), table = table.clone(), table_stats = ?new_stats, "finish compact");
                self.meta_api.update_background_task(UpdateBackgroundTaskReq{
                    task_name,
                    task_info: info.clone(),
                    expire_at: Utc::now().timestamp() as  u64 + EXPIRE_SEC,
                }).await?;
            }
            Err(e) => {
                info.message = format!("compaction failed: {:?}", e);
                Self::set_task_status(&mut info, BackgroundTaskState::FAILED);
                self.meta_api.update_background_task(UpdateBackgroundTaskReq{
                    task_name,
                    task_info: info.clone(),
                    expire_at: Utc::now().timestamp() as  u64 + EXPIRE_SEC,
                }).await?;
            }
        }
        Ok(())
    }

    // continuous compact on table until it's not needed
    // return true if actually compacted
    async fn do_compact_table(&self, svc: &Arc<BackgroundServiceHandlerWrapper>,
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
        debug!(
            job = "compaction",
            background = true,
            sql = sql.as_str(),
            "segment_compactor"
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
