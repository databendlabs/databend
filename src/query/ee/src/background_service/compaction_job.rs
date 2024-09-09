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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::BooleanArray;
use arrow_array::LargeStringArray;
use arrow_array::RecordBatch;
use arrow_array::UInt64Array;
use chrono::Utc;
use databend_common_base::base::tokio::sync::mpsc::Sender;
use databend_common_base::base::tokio::sync::Mutex;
use databend_common_base::base::tokio::time::Instant;
use databend_common_base::base::uuid::Uuid;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_meta_api::BackgroundApi;
use databend_common_meta_app::background::BackgroundJobIdent;
use databend_common_meta_app::background::BackgroundJobInfo;
use databend_common_meta_app::background::BackgroundJobParams;
use databend_common_meta_app::background::BackgroundJobStatus;
use databend_common_meta_app::background::BackgroundJobType::ONESHOT;
use databend_common_meta_app::background::BackgroundTaskIdent;
use databend_common_meta_app::background::BackgroundTaskInfo;
use databend_common_meta_app::background::BackgroundTaskState;
use databend_common_meta_app::background::GetBackgroundJobReq;
use databend_common_meta_app::background::ManualTriggerParams;
use databend_common_meta_app::background::UpdateBackgroundJobParamsReq;
use databend_common_meta_app::background::UpdateBackgroundJobStatusReq;
use databend_common_meta_app::background::UpdateBackgroundTaskReq;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_store::MetaStore;
use databend_common_meta_types::SeqV;
use databend_common_users::UserApiProvider;
use databend_query::sessions::QueryContext;
use databend_query::sessions::Session;
use databend_query::table_functions::SuggestedBackgroundTasksSource;
use log::debug;
use log::error;
use log::info;

use crate::background_service::job::Job;
use crate::background_service::session::create_session;

const BLOCK_COUNT: u64 = 500;
const PER_SEGMENT_BLOCK: u64 = 500;
const PER_BLOCK_SIZE: u64 = 50; // MB

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
    async fn run(&mut self) {
        info!(background = true, job_name :? =(&self.creator.clone()); "Compaction job started");
        self.do_compaction_job()
            .await
            .expect("failed to do compaction job");
    }

    async fn get_info(&self) -> Result<SeqV<BackgroundJobInfo>> {
        let job = self
            .meta_api
            .get_background_job(GetBackgroundJobReq {
                name: self.creator.clone(),
            })
            .await?;
        Ok(job.info)
    }

    fn get_name(&self) -> BackgroundJobIdent {
        self.creator.clone()
    }
    async fn update_job_status(&mut self, status: BackgroundJobStatus) -> Result<()> {
        self.meta_api
            .update_background_job_status(UpdateBackgroundJobStatusReq {
                job_name: self.creator.clone(),
                status: status.clone(),
            })
            .await?;
        Ok(())
    }

    async fn update_job_params(&mut self, param: BackgroundJobParams) -> Result<()> {
        self.meta_api
            .update_background_job_params(UpdateBackgroundJobParamsReq {
                job_name: self.creator.clone(),
                params: param.clone(),
            })
            .await?;
        Ok(())
    }
}

// continue to compact
pub fn should_continue_compaction(old: &TableStatistics, new: &TableStatistics) -> (bool, bool) {
    if old.number_of_blocks.is_none()
        || old.number_of_segments.is_none()
        || new.number_of_blocks.is_none()
        || new.number_of_segments.is_none()
    {
        return (false, false);
    }
    let old_segment_density =
        old.number_of_blocks.unwrap() as f64 / old.number_of_segments.unwrap() as f64;
    let new_segment_density =
        new.number_of_blocks.unwrap() as f64 / new.number_of_segments.unwrap() as f64;
    let should_continue_seg_compact = new_segment_density != old_segment_density
        && new.number_of_blocks.unwrap() > BLOCK_COUNT
        && new_segment_density < PER_SEGMENT_BLOCK as f64;
    let old_block_density = old.data_bytes as f64 / old.number_of_blocks.unwrap() as f64;
    let new_block_density = new.data_bytes as f64 / new.number_of_blocks.unwrap() as f64;
    let should_continue_blk_compact = new_block_density != old_block_density
        && new.number_of_blocks.unwrap() > BLOCK_COUNT
        && new_block_density < PER_BLOCK_SIZE as f64 * 1024.0 * 1024.0;
    (should_continue_seg_compact, should_continue_blk_compact)
}

// Service
// optimize table limit
// vacuum
impl CompactionJob {
    pub async fn create(
        config: &InnerConfig,
        name: impl ToString,
        finish_tx: Arc<Mutex<Sender<u64>>>,
    ) -> Result<Self> {
        let tenant = config.query.tenant_id.clone();

        let creator = BackgroundJobIdent::new(tenant, name);

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let j = Self {
            conf: config.clone(),
            meta_api,
            creator,
            finish_tx,
        };

        Ok(j)
    }
    async fn do_compaction_job(&mut self) -> Result<()> {
        let session = create_session(&self.conf).await?;
        let ctx = session.create_query_context().await?;
        let job_info = self.get_info().await?;

        let (params, manual) = Self::sync_compact_params(&job_info.data).await;
        // guarantee at least once for maunal job
        self.update_job_params(params).await?;

        for records in Self::do_get_target_tables_from_config(&self.conf, ctx.clone()).await? {
            debug!(records :? =(&records); "target_tables");
            let db_names = records
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap();
            let db_ids = records
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let tb_names = records
                .column(2)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            let tb_ids = records
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..records.num_rows() {
                let db_name = db_names.value(i).to_owned();
                let db_id = db_ids.value(i);
                let tb_name = tb_names.value(i).to_owned();
                let tb_id = tb_ids.value(i);
                match self
                    .compact_table(
                        session.clone(),
                        db_name.clone(),
                        tb_name.clone(),
                        db_id,
                        tb_id,
                        manual.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        info!(
                            "compaction job success, db: {}, table: {}",
                            db_name, tb_name
                        );
                    }
                    Err(e) => {
                        error!(
                            "compaction job failed, db: {}, table: {}, err: {}",
                            db_name, tb_name, e
                        );
                    }
                }
            }
        }
        info!(
            job = "compaction",
            background = true;
            "compaction task is done"
        );
        if self.conf.background.compaction.params.job_type == ONESHOT {
            let finish_tx = self.finish_tx.clone();
            let _ = finish_tx.lock().await.send(1).await;
        }

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

    async fn sync_compact_params(
        job_info: &BackgroundJobInfo,
    ) -> (BackgroundJobParams, Option<ManualTriggerParams>) {
        let mut job_params = job_info.job_params.clone().unwrap();
        let manual = job_params.manual_trigger_params.clone();
        job_params.manual_trigger_params = None;
        (job_params, manual)
    }
    async fn sync_compact_status(
        &self,
        id: String,
        job_info: &BackgroundJobInfo,
    ) -> Result<Option<BackgroundJobStatus>> {
        let mut job_status = job_info.job_status.clone().unwrap();
        job_status.last_task_id = Some(id);
        job_status.last_task_run_at = Some(Utc::now());
        job_status.next_task_scheduled_time = job_info
            .job_params
            .as_ref()
            .unwrap()
            .get_next_running_time(job_status.last_task_run_at.unwrap());
        Ok(Some(job_status))
    }

    async fn compact_table(
        &mut self,
        session: Arc<Session>,
        database: String,
        table: String,
        db_id: u64,
        tb_id: u64,
        manual: Option<ManualTriggerParams>,
    ) -> Result<()> {
        let (seg, blk, stats) = Self::do_check_table(
            session.clone(),
            database.clone(),
            table.clone(),
            BLOCK_COUNT,
            PER_SEGMENT_BLOCK,
            PER_BLOCK_SIZE,
        )
        .await?;
        let (seg, blk, stats) = if !self.conf.background.compaction.has_target_tables() {
            if !seg && !blk {
                info!(job = "compaction", background = true, database = database.clone(), table = table.clone(), should_compact_segment = seg, should_compact_blk = blk, table_stats :? =(&stats); "skip compact");
                return Ok(());
            }
            (seg, blk, stats)
        } else {
            (true, true, stats)
        };

        if !seg && !blk {
            info!(job = "compaction", background = true, database = database.clone(), table = table.clone(), should_compact_segment = seg, should_compact_blk = blk, table_stats :? =(&stats); "skip compact");
            return Ok(());
        }
        let job_info = self.get_info().await?;
        let id = Uuid::new_v4().to_string();

        let status = self.sync_compact_status(id.clone(), &job_info.data).await?;
        if status.is_none() {
            return Ok(());
        }
        self.update_job_status(status.clone().unwrap()).await?;

        info!(job = "compaction", background = true, id=id.clone(), database = database.clone(), table = table.clone(), should_compact_segment = seg, should_compact_blk = blk, table_stats :? =(&stats); "start compact");
        let task_ident = BackgroundTaskIdent::new(
            self.creator.tenant().clone(),
            status.unwrap().last_task_id.unwrap(),
        );
        let mut info = BackgroundTaskInfo::new_compaction_task(
            self.creator.clone(),
            db_id,
            tb_id,
            stats,
            manual,
            format!(
                "need segment compaction: {}, need block compaction: {}",
                seg, blk
            ),
        );
        self.meta_api
            .update_background_task(UpdateBackgroundTaskReq {
                task_name: task_ident.clone(),
                task_info: info.clone(),
                expire_at: Utc::now().timestamp() as u64 + EXPIRE_SEC,
            })
            .await?;

        let start = Instant::now();

        match self
            .do_compact_table(session.clone(), database.clone(), table.clone())
            .await
        {
            Ok(_) => {
                let (_, _, new_stats) = Self::do_check_table(
                    session.clone(),
                    database.clone(),
                    table.clone(),
                    BLOCK_COUNT,
                    PER_SEGMENT_BLOCK,
                    PER_BLOCK_SIZE,
                )
                .await?;
                Self::set_task_stats(&mut info, new_stats.clone(), start.elapsed());
                Self::set_task_status(&mut info, BackgroundTaskState::DONE);
                info!(job = "compaction", background = true, id=id.clone(), database = database.clone(), table = table.clone(), table_stats :? =(&new_stats); "finish compact");
                self.meta_api
                    .update_background_task(UpdateBackgroundTaskReq {
                        task_name: task_ident,
                        task_info: info.clone(),
                        expire_at: Utc::now().timestamp() as u64 + EXPIRE_SEC,
                    })
                    .await?;
            }
            Err(e) => {
                info.message = format!("compaction failed: {:?}", e);
                Self::set_task_status(&mut info, BackgroundTaskState::FAILED);
                self.meta_api
                    .update_background_task(UpdateBackgroundTaskReq {
                        task_name: task_ident,
                        task_info: info.clone(),
                        expire_at: Utc::now().timestamp() as u64 + EXPIRE_SEC,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    // continuous compact on table until it's not needed
    // return true if actually compacted
    async fn do_compact_table(
        &self,
        session: Arc<Session>,
        database: String,
        table: String,
    ) -> Result<bool> {
        let (seg, blk, stats) = Self::do_check_table(
            session.clone(),
            database.clone(),
            table.clone(),
            BLOCK_COUNT,
            PER_SEGMENT_BLOCK,
            PER_BLOCK_SIZE,
        )
        .await?;
        if !seg && !blk {
            return Ok(false);
        }

        let mut old = stats;
        if seg {
            loop {
                self.do_segment_compaction(
                    session.clone(),
                    database.clone(),
                    table.clone(),
                    self.conf.background.compaction.segment_limit,
                )
                .await?;
                let (_, _, new) = Self::do_check_table(
                    session.clone(),
                    database.clone(),
                    table.clone(),
                    BLOCK_COUNT,
                    PER_SEGMENT_BLOCK,
                    PER_BLOCK_SIZE,
                )
                .await?;
                if !should_continue_compaction(&old, &new).0 {
                    old = new;
                    break;
                }
                old = new;
            }
        }

        if blk {
            loop {
                self.do_block_compaction(
                    session.clone(),
                    database.clone(),
                    table.clone(),
                    self.conf.background.compaction.block_limit,
                )
                .await?;
                let (_, _, new) = Self::do_check_table(
                    session.clone(),
                    database.clone(),
                    table.clone(),
                    BLOCK_COUNT,
                    PER_SEGMENT_BLOCK,
                    PER_BLOCK_SIZE,
                )
                .await?;
                if !should_continue_compaction(&old, &new).1 {
                    break;
                }
                old = new;
            }
        }

        Ok(true)
    }

    pub async fn do_get_target_tables_from_config(
        config: &InnerConfig,
        ctx: Arc<QueryContext>,
    ) -> Result<Vec<RecordBatch>> {
        if !config.background.compaction.has_target_tables() {
            let res =
                SuggestedBackgroundTasksSource::do_get_all_suggested_compaction_tables(ctx).await;
            return res;
        }
        Self::do_get_target_tables(config, ctx).await
    }
    // format a vec of tables into a sql string
    // vec!("t1", "t2", "t3")
    // into
    // "('t1', 't2', 't3')"
    fn format_tables(tables: Vec<String>) -> String {
        let formatted_tables: Vec<String> =
            tables.iter().map(|table| format!("'{}'", table)).collect();

        format!("({})", formatted_tables.join(", "))
    }
    pub fn get_target_from_config_sql(database: String, tables: Vec<String>) -> String {
        format!(
         "
        SELECT t.database as database, d.database_id as database_id, t.name as table, t.table_id as table_id
        FROM system.tables as t
        JOIN system.databases as d
        ON t.database = d.name
        WHERE t.database != 'system'
            AND t.database != 'information_schema'
            AND t.engine = 'FUSE'
            AND t.database = '{}'
            AND t.name IN {}
            ;
        ",
        database,
        Self::format_tables(tables)
        )
    }

    pub fn parse_all_target_tables(original: Option<&Vec<String>>) -> HashMap<String, Vec<String>> {
        let mut result: HashMap<String, Vec<String>> = HashMap::new();

        if let Some(original_vec) = original {
            for entry in original_vec {
                let parts: Vec<&str> = entry.split('.').collect();

                match parts.len() {
                    1 => {
                        // Only table name provided, assuming no database
                        let table = parts[0].to_owned();
                        result.entry("default".to_owned()).or_default().push(table);
                    }
                    2 => {
                        // Both database and table names provided
                        let db = parts[0].to_owned();
                        let table = parts[1].to_owned();
                        result.entry(db).or_default().push(table);
                    }
                    _ => {
                        // Invalid input, skipping entry
                    }
                }
            }
        }

        result
    }

    pub async fn do_get_target_tables(
        configs: &InnerConfig,
        ctx: Arc<QueryContext>,
    ) -> Result<Vec<RecordBatch>> {
        let all_target_tables = configs.background.compaction.target_tables.as_ref();
        let all_target_tables = Self::parse_all_target_tables(all_target_tables);
        let future_res = all_target_tables
            .into_iter()
            .map(|(k, v)| {
                let sql = Self::get_target_from_config_sql(k, v);
                info!(
                    job = "compaction",
                    background = true,
                    sql = sql.as_str();
                    "get target tables"
                );
                SuggestedBackgroundTasksSource::do_execute_sql(ctx.clone(), sql)
            })
            .collect::<Vec<_>>();
        let mut res = Vec::new();
        for f in future_res {
            res.push(f.await?);
        }
        let res = res.into_iter().flatten().collect::<Vec<_>>();
        Ok(res)
    }

    pub async fn do_check_table(
        session: Arc<Session>,
        database: String,
        table: String,
        blk_cnt: u64,
        avg_seg: u64,
        avg_blk: u64,
    ) -> Result<(bool, bool, TableStatistics)> {
        let sql = Self::get_compaction_advice_sql(database, table, blk_cnt, avg_seg, avg_blk);
        debug!(
            job = "compaction",
            background = true,
            sql = sql.as_str();
            "check target_table"
        );
        let ctx = session.create_query_context().await?;
        let res = SuggestedBackgroundTasksSource::do_execute_sql(ctx, sql).await?;
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
        session: Arc<Session>,
        database: String,
        table: String,
        limit: Option<u64>,
    ) -> Result<()> {
        let sql = Self::get_segment_compaction_sql(database, table, limit);
        debug!(
            job = "compaction",
            background = true,
            sql = sql.as_str();
            "segment_compactor"
        );
        let ctx = session.create_query_context().await?;
        SuggestedBackgroundTasksSource::do_execute_sql(ctx, sql).await?;
        Ok(())
    }

    async fn do_block_compaction(
        &self,
        session: Arc<Session>,
        database: String,
        table: String,
        limit: Option<u64>,
    ) -> Result<()> {
        let sql = Self::get_block_compaction_sql(database, table, limit);
        debug!(
            job = "compaction",
            background = true,
            sql = sql.as_str();
            "block_compaction"
        );
        let ctx = session.create_query_context().await?;
        SuggestedBackgroundTasksSource::do_execute_sql(ctx, sql).await?;
        Ok(())
    }

    pub fn get_compaction_advice_sql(
        database: String,
        table: String,
        blk_count: u64,
        avg_seg: u64,
        avg_blk: u64,
    ) -> String {
        format!(
            "
        select
        IF(block_count > {} and block_count / segment_count < {}, TRUE, FALSE) AS segment_advice,
        IF(row_count > 100000 AND block_count > {} AND bytes_uncompressed / block_count / 1024 / 1024  < {}, TRUE, FALSE) AS block_advice,
        row_count, bytes_uncompressed, bytes_compressed, index_size,
        segment_count, block_count,
        block_count/segment_count
        from fuse_snapshot('{}', '{}') order by timestamp ASC LIMIT 1;
        ",
            blk_count, avg_seg, blk_count, avg_blk, database, table
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
