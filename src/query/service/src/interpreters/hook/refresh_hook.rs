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

// Logs from this module will show up as "[REFRESH-HOOK] ...".
databend_common_tracing::register_module_tag!("[REFRESH-HOOK]");

use std::sync::Arc;

use databend_common_ast::ast;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::BindContext;
use databend_common_sql::Binder;
use databend_common_sql::Metadata;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RefreshIndexPlan;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_meta_types::MetaId;
use databend_storages_common_table_meta::meta::Location;
use log::info;
use parking_lot::RwLock;

use crate::interpreters::Interpreter;
use crate::interpreters::RefreshIndexInterpreter;
use crate::interpreters::RefreshTableIndexInterpreter;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;

pub struct RefreshDesc {
    pub catalog: String,
    pub database: String,
    pub table: String,
}

/// Hook refresh action with a on-finished callback.
/// errors (if any) are ignored.
pub async fn hook_refresh(ctx: Arc<QueryContext>, pipeline: &mut Pipeline, desc: RefreshDesc) {
    if pipeline.is_empty() {
        return;
    }

    pipeline.set_on_finished(move |info: &ExecutionInfo| {
        if info.res.is_ok() {
            info!("Pipeline execution completed successfully, starting refresh job");
            match GlobalIORuntime::instance().block_on(do_refresh(ctx, desc)) {
                Ok(_) => {
                    info!("Refresh job completed successfully");
                }
                Err(e) => {
                    info!("Refresh job failed: {:?}", e);
                }
            }
        }
        Ok(())
    });
}

async fn do_refresh(ctx: Arc<QueryContext>, desc: RefreshDesc) -> Result<()> {
    let table = ctx
        .get_table(&desc.catalog, &desc.database, &desc.table)
        .await?;
    let table_id = table.get_id();

    ctx.clear_table_meta_timestamps_cache();

    let mut plans = Vec::new();

    // Generate sync aggregating indexes.
    if ctx
        .get_settings()
        .get_enable_refresh_aggregating_index_after_write()?
    {
        let agg_index_plans =
            generate_refresh_index_plan(ctx.clone(), &desc.catalog, table_id).await?;
        plans.extend_from_slice(&agg_index_plans);
    }

    // Generate sync inverted indexes.
    let inverted_index_plans = generate_refresh_table_index_plan(ctx.clone(), &desc, table).await?;
    plans.extend_from_slice(&inverted_index_plans);

    let mut tasks = Vec::with_capacity(std::cmp::min(
        ctx.get_settings().get_max_threads()? as usize,
        plans.len(),
    ));

    for plan in plans {
        let ctx_cloned = ctx.clone();
        tasks.push(async move {
            match plan {
                Plan::RefreshIndex(agg_index_plan) => {
                    let refresh_agg_index_interpreter =
                        RefreshIndexInterpreter::try_create(ctx_cloned.clone(), *agg_index_plan)?;
                    let mut build_res = refresh_agg_index_interpreter.execute2().await?;
                    if build_res.main_pipeline.is_empty() {
                        return Ok(());
                    }

                    let settings = ctx_cloned.get_settings();
                    build_res.set_max_threads(settings.get_max_threads()? as usize);
                    let settings = ExecutorSettings::try_create(ctx_cloned.clone())?;

                    if build_res.main_pipeline.is_complete_pipeline()? {
                        let query_ctx = ctx_cloned.clone();
                        build_res.main_pipeline.set_on_finished(always_callback(
                            move |_: &ExecutionInfo| {
                                hook_clear_m_cte_temp_table(&query_ctx)?;
                                hook_vacuum_temp_files(&query_ctx)?;
                                hook_disk_temp_dir(&query_ctx)?;
                                Ok(())
                            },
                        ));

                        let mut pipelines = build_res.sources_pipelines;
                        pipelines.push(build_res.main_pipeline);

                        let complete_executor =
                            PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
                        ctx_cloned.set_executor(complete_executor.get_inner())?;
                        complete_executor.execute()
                    } else {
                        Ok(())
                    }
                }
                Plan::RefreshTableIndex(inverted_index_plan) => {
                    let refresh_inverted_index_interpreter =
                        RefreshTableIndexInterpreter::try_create(
                            ctx_cloned.clone(),
                            *inverted_index_plan,
                        )?;
                    let mut build_res = refresh_inverted_index_interpreter.execute2().await?;
                    if build_res.main_pipeline.is_empty() {
                        return Ok(());
                    }

                    let settings = ctx_cloned.get_settings();
                    build_res.set_max_threads(settings.get_max_threads()? as usize);
                    let settings = ExecutorSettings::try_create(ctx_cloned.clone())?;

                    if build_res.main_pipeline.is_complete_pipeline()? {
                        let query_ctx = ctx_cloned.clone();
                        build_res.main_pipeline.set_on_finished(always_callback(
                            move |_info: &ExecutionInfo| {
                                hook_clear_m_cte_temp_table(&query_ctx)?;
                                hook_vacuum_temp_files(&query_ctx)?;
                                hook_disk_temp_dir(&query_ctx)?;
                                Ok(())
                            },
                        ));

                        let mut pipelines = build_res.sources_pipelines;
                        pipelines.push(build_res.main_pipeline);

                        let complete_executor =
                            PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
                        ctx_cloned.set_executor(complete_executor.get_inner())?;
                        complete_executor.execute()
                    } else {
                        Ok(())
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    let _ = futures::future::try_join_all(tasks).await?;
    Ok(())
}

async fn generate_refresh_index_plan(
    ctx: Arc<QueryContext>,
    catalog: &str,
    table_id: MetaId,
) -> Result<Vec<Plan>> {
    let segment_locs = ctx.get_written_segment_locations()?;
    let catalog = ctx.get_catalog(catalog).await?;
    let mut plans = vec![];
    let indexes = catalog
        .list_indexes_by_table_id(ListIndexesByIdReq::new(ctx.get_tenant(), table_id))
        .await?;

    let sync_indexes = indexes
        .into_iter()
        .filter(|(_, _, meta)| meta.sync_creation)
        .collect::<Vec<_>>();

    for (index_id, index_name, index_meta) in sync_indexes {
        let plan = build_refresh_index_plan(
            ctx.clone(),
            index_id,
            index_name,
            index_meta,
            segment_locs.clone(),
        )
        .await?;
        plans.push(Plan::RefreshIndex(Box::new(plan)));
    }

    Ok(plans)
}

async fn build_refresh_index_plan(
    ctx: Arc<QueryContext>,
    index_id: u64,
    index_name: String,
    index_meta: IndexMeta,
    segment_locs: Vec<Location>,
) -> Result<RefreshIndexPlan> {
    let settings = ctx.get_settings();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;

    let mut binder = Binder::new(
        ctx.clone(),
        CatalogManager::instance(),
        name_resolution_ctx,
        metadata.clone(),
    );
    let mut bind_context = BindContext::new();

    binder
        .build_refresh_index_plan(
            &mut bind_context,
            index_id,
            index_name,
            index_meta,
            None,
            Some(segment_locs),
        )
        .await
}

async fn generate_refresh_table_index_plan(
    ctx: Arc<QueryContext>,
    desc: &RefreshDesc,
    table: Arc<dyn Table>,
) -> Result<Vec<Plan>> {
    let segment_locs = ctx.get_written_segment_locations()?;
    let mut plans = vec![];

    let table_meta = &table.get_table_info().meta;
    for (_, index) in table_meta.indexes.iter() {
        if index.sync_creation {
            continue;
        }
        let index_type = match index.index_type {
            TableIndexType::Inverted => ast::TableIndexType::Inverted,
            TableIndexType::Ngram => ast::TableIndexType::Ngram,
            TableIndexType::Vector => ast::TableIndexType::Vector,
            TableIndexType::Spatial => ast::TableIndexType::Spatial,
        };
        let plan = RefreshTableIndexPlan {
            index_type,
            catalog: desc.catalog.clone(),
            database: desc.database.clone(),
            table: desc.table.clone(),
            index_name: index.name.clone(),
            segment_locs: Some(segment_locs.clone()),
        };
        plans.push(Plan::RefreshTableIndex(Box::new(plan)));
    }
    Ok(plans)
}
