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

use common_base::runtime::GlobalIORuntime;
use common_catalog::catalog::CatalogManager;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::ListIndexesByIdReq;
use common_meta_types::MetaId;
use common_pipeline_core::Pipeline;
use common_sql::plans::RefreshIndexPlan;
use common_sql::BindContext;
use common_sql::Binder;
use common_sql::Metadata;
use common_sql::NameResolutionContext;
use log::info;
use parking_lot::RwLock;
use storages_common_table_meta::meta::Location;

use crate::interpreters::Interpreter;
use crate::interpreters::RefreshIndexInterpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;

pub struct RefreshAggIndexDesc {
    pub catalog: String,
    pub database: String,
    pub table: String,
}

pub async fn hook_refresh_agg_index(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    desc: RefreshAggIndexDesc,
) -> Result<()> {
    if pipeline.is_empty() {
        return Ok(());
    }

    if ctx
        .get_settings()
        .get_enable_refresh_aggregating_index_after_write()?
    {
        pipeline.set_on_finished(move |err| {
            if err.is_none() {
                info!("execute pipeline finished successfully, starting run generate aggregating index job.");
                match GlobalIORuntime::instance().block_on({
                    refresh_agg_index(ctx, desc)
                }) {
                    Ok(_) => info!("execute generate aggregating index job successfully."),
                    Err(e) => info!("execute generate aggregating index job failed: {:?}", e),
                }
            }
            Ok(())
        });
    }

    Ok(())
}

async fn generate_refresh_index_plan(
    ctx: Arc<QueryContext>,
    catalog: &str,
    table_id: MetaId,
) -> Result<Vec<RefreshIndexPlan>> {
    let segment_locs = ctx.get_segment_locations()?;
    let catalog = ctx.get_catalog(catalog).await?;
    let mut plans = vec![];
    let indexes = catalog
        .list_indexes_by_table_id(ListIndexesByIdReq {
            tenant: ctx.get_tenant(),
            table_id,
        })
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
        plans.push(plan);
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

async fn refresh_agg_index(ctx: Arc<QueryContext>, desc: RefreshAggIndexDesc) -> Result<()> {
    let table_id = ctx
        .get_table(&desc.catalog, &desc.database, &desc.table)
        .await?
        .get_id();
    let plans = generate_refresh_index_plan(ctx.clone(), &desc.catalog, table_id).await?;
    let mut tasks = Vec::with_capacity(std::cmp::min(
        ctx.get_settings().get_max_threads()? as usize,
        plans.len(),
    ));
    for plan in plans {
        let ctx_cloned = ctx.clone();
        tasks.push(async move {
            let refresh_agg_index_interpreter =
                RefreshIndexInterpreter::try_create(ctx_cloned.clone(), plan)?;
            let mut build_res = refresh_agg_index_interpreter.execute2().await?;
            if build_res.main_pipeline.is_empty() {
                return Ok(());
            }

            let settings = ctx_cloned.get_settings();
            let query_id = ctx_cloned.get_id();
            build_res.set_max_threads(settings.get_max_threads()? as usize);
            let settings = ExecutorSettings::try_create(&settings, query_id)?;

            if build_res.main_pipeline.is_complete_pipeline()? {
                let mut pipelines = build_res.sources_pipelines;
                pipelines.push(build_res.main_pipeline);

                let complete_executor =
                    PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
                ctx_cloned.set_executor(complete_executor.get_inner())?;
                complete_executor.execute()
            } else {
                Ok(())
            }
        });
    }
    let _ = futures::future::try_join_all(tasks).await?;
    Ok(())
}
