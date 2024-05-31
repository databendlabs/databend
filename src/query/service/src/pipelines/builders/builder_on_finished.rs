// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_storages_stage::StageTable;
use databend_storages_common_io::Files;
use log::error;
use log::info;

use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

impl PipelineBuilder {
    pub fn set_purge_files_on_finished(
        ctx: Arc<QueryContext>,
        files: Vec<String>,
        copy_purge_option: bool,
        stage_info: StageInfo,
        main_pipeline: &mut Pipeline,
    ) -> Result<()> {
        let is_active = {
            let txn_mgr = ctx.txn_mgr();
            let mut txn_mgr = txn_mgr.lock();
            let is_active = txn_mgr.is_active();
            if is_active && copy_purge_option {
                txn_mgr.add_need_purge_files(stage_info.clone(), files.clone());
            }
            is_active
        };
        // set on_finished callback.
        main_pipeline.set_on_finished(move |info: &ExecutionInfo| {
            match &info.res {
                Ok(_) => {
                    GlobalIORuntime::instance().block_on(async move {
                        // 1. log on_error mode errors.
                        // todo(ariesdevil): persist errors with query_id
                        if let Some(error_map) = ctx.get_maximum_error_per_file() {
                            for (file_name, e) in error_map {
                                error!(
                                    "copy(on_error={}): file {} encounter error {},",
                                    stage_info.copy_options.on_error,
                                    file_name,
                                    e.to_string()
                                );
                            }
                        }

                        // 2. Try to purge copied files if purge option is true, if error will skip.
                        // If a file is already copied(status with AlreadyCopied) we will try to purge them.
                        if !is_active && copy_purge_option {
                            Self::try_purge_files(ctx.clone(), &stage_info, &files).await;
                        }

                        Ok(())
                    })?;
                }
                Err(error) => {
                    error!("copy failed, reason: {}", error);
                }
            }
            Ok(())
        });
        Ok(())
    }

    pub async fn purge_files_immediately(
        ctx: Arc<QueryContext>,
        files: Vec<String>,
        stage_info: StageInfo,
    ) -> Result<()> {
        {
            // if we are in an ongoing transaction,
            // we just add the files to the need_purge_files list and return
            let txn_mgr = ctx.txn_mgr();
            let mut txn_mgr = txn_mgr.lock();
            let is_active = txn_mgr.is_active();
            if is_active {
                txn_mgr.add_need_purge_files(stage_info.clone(), files.clone());
                return Ok(());
            }
        }

        Self::try_purge_files(ctx.clone(), &stage_info, &files).await;

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn try_purge_files(ctx: Arc<QueryContext>, stage_info: &StageInfo, files: &[String]) {
        let start = Instant::now();
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let op = StageTable::get_op(stage_info);

        match op {
            Ok(op) => {
                let file_op = Files::create(table_ctx, op);
                if let Err(e) = file_op.remove_file_in_batch(files).await {
                    error!("Failed to delete file: {:?}, error: {}", files, e);
                }
            }
            Err(e) => {
                error!("Failed to get stage table op, error: {}", e);
            }
        }

        let elapsed = start.elapsed();
        info!(
            "purged files: number {}, time used {:?} ",
            files.len(),
            elapsed
        );

        // Perf.
        {
            metrics_inc_copy_purge_files_counter(files.len() as u32);
            metrics_inc_copy_purge_files_cost_milliseconds(elapsed.as_millis() as u32);
        }
    }
}
