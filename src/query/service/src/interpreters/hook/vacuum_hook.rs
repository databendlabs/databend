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

// Logs from this module will show up as "[VACUUM-HOOK] ...".
databend_common_tracing::register_module_tag!("[VACUUM-HOOK]");

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
use databend_storages_common_cache::TempDirManager;
use log::warn;
use rand::Rng;

use crate::clusters::ClusterHelper;
use crate::sessions::QueryContext;

pub fn hook_vacuum_temp_files(query_ctx: &Arc<QueryContext>) -> Result<()> {
    let settings = query_ctx.get_settings();
    let spill_prefix = query_ctx.query_tenant_spill_prefix();
    let vacuum_limit = settings.get_max_vacuum_temp_files_after_query()?;

    // disable all s3 operator if vacuum limit = 0
    if vacuum_limit != 0
        && LicenseManagerSwitch::instance()
            .check_enterprise_enabled(query_ctx.get_license_key(), Vacuum)
            .is_ok()
    {
        let handler = get_vacuum_handler();

        let cluster = query_ctx.get_cluster();
        let query_id = query_ctx.get_id();
        let abort_checker = query_ctx.clone().get_abort_checker();
        let mut node_files = HashMap::new();
        for node in cluster.nodes.iter() {
            let stats = query_ctx.get_spill_file_stats(Some(node.id.clone()));
            // if query was aborted, we can't trust the stats
            if stats.file_nums != 0 || abort_checker.try_check_aborting().is_err() {
                if let Some(index) = cluster.index_of_nodeid(&node.id) {
                    node_files.insert(index, stats.file_nums);
                }
            }
        }
        if node_files.is_empty() {
            return Ok(());
        }

        log::info!("Cleaning temporary files from nodes: {:?}", node_files);

        let nodes = node_files.keys().cloned().collect::<Vec<usize>>();
        let _ = GlobalIORuntime::instance().block_on::<(), ErrorCode, _>(async move {
            let removed_files = handler
                .do_vacuum_temporary_files(
                    abort_checker,
                    spill_prefix.clone(),
                    &VacuumTempOptions::QueryHook(nodes, query_id),
                    vacuum_limit as usize,
                )
                .await;

            if let Err(cause) = &removed_files {
                log::warn!("Failed to clean temporary files: {:?}", cause);
            }

            Ok(())
        });
    }

    Ok(())
}

pub fn hook_disk_temp_dir(query_ctx: &Arc<QueryContext>) -> Result<()> {
    if matches!(
        query_ctx.get_current_session().get_type(),
        SessionType::HTTPQuery
    ) {
        // HTTP queries may still stream spilled result pages after execution finishes.
        // Cleanup is deferred to the HTTP query lifecycle.
        return Ok(());
    }

    let mgr = TempDirManager::instance();

    if mgr.drop_disk_spill_dir(&query_ctx.get_id())? && rand::thread_rng().gen_ratio(1, 10) {
        let limit = query_ctx
            .get_settings()
            .get_spilling_to_disk_vacuum_unknown_temp_dirs_limit()?;
        let deleted = mgr.drop_disk_spill_dir_unknown(limit)?;
        if !deleted.is_empty() {
            warn!("Removed residual temporary directories: {:?}", deleted)
        }
    }

    Ok(())
}

pub fn hook_clear_m_cte_temp_table(query_ctx: &Arc<QueryContext>) -> Result<()> {
    let _ = GlobalIORuntime::instance().block_on(async move {
        query_ctx.drop_m_cte_temp_table().await?;
        Ok(())
    });
    Ok(())
}
