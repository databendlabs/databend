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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::always_callback;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_sql::bind_table;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::CommitType;
use databend_common_sql::executor::physical_plans::CompactSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::HilbertPartition;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::Recluster;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::plan_hilbert_sql;
use databend_common_sql::plans::replace_with_constant;
use databend_common_sql::plans::set_update_stream_columns;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::ReclusterPlan;
use databend_common_sql::query_executor::QueryExecutor;
use databend_common_sql::IdentifierNormalizer;
use databend_common_sql::MetadataRef;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeChecker;
use databend_common_storages_fuse::DEFAULT_BLOCK_PER_SEGMENT;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_enterprise_hilbert_clustering::get_hilbert_clustering_handler;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ClusterType;
use derive_visitor::DriveMut;
use log::error;
use log::warn;

use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::interpreters::interpreter_insert_multi_table::scalar_expr_to_remote_expr;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ReclusterTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ReclusterPlan,
    lock_opt: LockTableOption,
}

impl ReclusterTableInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: ReclusterPlan,
        lock_opt: LockTableOption,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            plan,
            lock_opt,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for ReclusterTableInterpreter {
    fn name(&self) -> &str {
        "ReclusterTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let ctx = self.ctx.clone();
        let recluster_timeout_secs = ctx.get_settings().get_recluster_timeout_secs()?;

        let mut times = 0;
        let mut push_downs = None;
        let mut hilbert_info = None;
        let start = SystemTime::now();
        let timeout = Duration::from_secs(recluster_timeout_secs);
        let is_final = self.plan.is_final;
        loop {
            if let Err(err) = ctx.check_aborting() {
                error!(
                    "execution of recluster statement aborted. server is shutting down or the query was killed",
                );
                return Err(err.with_context("failed to execute"));
            }

            let res = self
                .execute_recluster(&mut push_downs, &mut hilbert_info)
                .await;

            match res {
                Ok(is_break) => {
                    if is_break {
                        break;
                    }
                }
                Err(e) => {
                    if is_final
                        && matches!(
                            e.code(),
                            ErrorCode::TABLE_LOCK_EXPIRED
                                | ErrorCode::TABLE_ALREADY_LOCKED
                                | ErrorCode::TABLE_VERSION_MISMATCHED
                                | ErrorCode::UNRESOLVABLE_CONFLICT
                        )
                    {
                        warn!("Execute recluster error: {:?}", e);
                    } else {
                        return Err(e);
                    }
                }
            }

            let elapsed_time = SystemTime::now().duration_since(start).unwrap();
            times += 1;
            // Status.
            {
                let status = format!(
                    "recluster: run recluster tasks:{} times, cost:{:?}",
                    times, elapsed_time
                );
                ctx.set_status_info(&status);
            }

            if !is_final {
                break;
            }

            if elapsed_time >= timeout {
                warn!(
                    "Recluster stopped because the runtime was over {:?}",
                    timeout
                );
                break;
            }
        }

        Ok(PipelineBuildResult::create())
    }
}

impl ReclusterTableInterpreter {
    async fn execute_recluster(
        &self,
        push_downs: &mut Option<PushDownInfo>,
        hilbert_info: &mut Option<HilbertBuildInfo>,
    ) -> Result<bool> {
        let start = SystemTime::now();
        let settings = self.ctx.get_settings();

        let ReclusterPlan {
            catalog,
            database,
            table,
            limit,
            ..
        } = &self.plan;
        // try to add lock table.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(catalog, database, table, &self.lock_opt)
            .await?;

        let tbl = self.ctx.get_table(catalog, database, table).await?;
        // check mutability
        tbl.check_mutable()?;
        let Some(cluster_type) = tbl.cluster_type() else {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table '{}.{}'",
                database, table,
            )));
        };

        self.build_push_downs(push_downs, &tbl)?;

        let physical_plan = match cluster_type {
            ClusterType::Hilbert => {
                self.build_hilbert_plan(&tbl, push_downs, hilbert_info)
                    .await?
            }
            ClusterType::Linear => self.build_linear_plan(&tbl, push_downs, *limit).await?,
        };
        let Some(mut physical_plan) = physical_plan else {
            return Ok(true);
        };
        physical_plan.adjust_plan_id(&mut 0);
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        {
            let ctx = self.ctx.clone();
            let catalog = self.plan.catalog.clone();
            let database = self.plan.database.clone();
            let table = self.plan.table.clone();
            build_res.main_pipeline.set_on_finished(always_callback(
                move |info: &ExecutionInfo| {
                    ctx.clear_written_segment_locations()?;
                    ctx.clear_selected_segment_locations();
                    ctx.evict_table_from_cache(&catalog, &database, &table)?;

                    ctx.unload_spill_meta();
                    hook_clear_m_cte_temp_table(&ctx)?;
                    hook_vacuum_temp_files(&ctx)?;
                    hook_disk_temp_dir(&ctx)?;
                    match &info.res {
                        Ok(_) => {
                            InterpreterClusteringHistory::write_log(
                                &ctx, start, &database, &table,
                            )?;

                            Ok(())
                        }
                        Err(error) => Err(error.clone()),
                    }
                },
            ));
        }

        debug_assert!(build_res.main_pipeline.is_complete_pipeline()?);

        let max_threads = settings.get_max_threads()? as usize;
        build_res.set_max_threads(max_threads);

        let executor_settings = ExecutorSettings::try_create(self.ctx.clone())?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor =
            PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
        self.ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;

        // make sure the executor is dropped before the next loop.
        drop(complete_executor);
        // make sure the lock guard is dropped before the next loop.
        drop(lock_guard);

        Ok(false)
    }

    async fn build_hilbert_plan(
        &self,
        tbl: &Arc<dyn Table>,
        push_downs: &mut Option<PushDownInfo>,
        hilbert_info: &mut Option<HilbertBuildInfo>,
    ) -> Result<Option<PhysicalPlan>> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::HilbertClustering)?;
        let handler = get_hilbert_clustering_handler();
        let Some((recluster_info, snapshot)) = handler
            .do_hilbert_clustering(tbl.clone(), self.ctx.clone(), push_downs.clone())
            .await?
        else {
            return Ok(None);
        };

        let settings = self.ctx.get_settings();
        let table_info = tbl.get_table_info().clone();

        let block_thresholds = tbl.get_block_thresholds();
        let block_per_seg = table_info
            .options()
            .get(FUSE_OPT_KEY_BLOCK_PER_SEGMENT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_BLOCK_PER_SEGMENT);

        let total_bytes = recluster_info.removed_statistics.uncompressed_byte_size as usize;
        let total_rows = recluster_info.removed_statistics.row_count as usize;

        let rows_per_block =
            block_thresholds.calc_rows_per_block(total_bytes, total_rows, Some(block_per_seg));
        let total_partitions = std::cmp::max(total_rows / rows_per_block, 1);
        warn!("Do hilbert recluster, total_bytes: {}, total_rows: {}, total_partitions: {}, rows_per_block: {}",
            total_bytes, total_rows, total_partitions, rows_per_block);

        let subquery_executor = Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
            self.ctx.as_ref(),
        )));
        let partitions = settings.get_hilbert_num_range_ids()? as usize;

        self.build_hilbert_info(tbl, hilbert_info).await?;
        let HilbertBuildInfo {
            keys_bound,
            index_bound,
            query,
        } = hilbert_info.as_ref().unwrap();

        let mut variables = VecDeque::new();

        let keys_bounds = self
            .execute_hilbert_plan(
                &subquery_executor,
                keys_bound,
                std::cmp::max(total_partitions, partitions),
                &variables,
                tbl,
            )
            .await?;
        for entry in keys_bounds.columns().iter() {
            let v = entry.value.index(0).unwrap().to_owned();
            variables.push_back(v);
        }

        let index_bounds = self
            .execute_hilbert_plan(
                &subquery_executor,
                index_bound,
                total_partitions,
                &variables,
                tbl,
            )
            .await?;
        let val = index_bounds.value_at(0, 0).unwrap().to_owned();
        variables.push_front(val);

        // reset the scan progress.
        self.ctx.get_scan_progress().set(&Default::default());
        let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } = query
        else {
            unreachable!()
        };
        let mut s_expr = replace_with_constant(s_expr, &variables, total_partitions as u16);
        if tbl.change_tracking_enabled() {
            s_expr = set_update_stream_columns(&s_expr)?;
        }
        metadata.write().replace_all_tables(tbl.clone());
        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
        let mut plan = Box::new(builder.build(&s_expr, bind_context.column_set()).await?);

        let mut is_exchange = false;
        if let PhysicalPlan::Exchange(Exchange {
            input,
            kind: FragmentKind::Merge,
            ..
        }) = plan.as_ref()
        {
            is_exchange = true;
            plan = input.clone();
        }

        let cluster = self.ctx.get_cluster();
        let is_distributed = is_exchange || !cluster.is_empty();
        if is_distributed {
            let expr = scalar_expr_to_remote_expr(
                &ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: bind_context.columns.last().unwrap().clone(),
                }),
                plan.output_schema()?.as_ref(),
            )?;
            plan = Box::new(PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: plan,
                kind: FragmentKind::Normal,
                keys: vec![expr],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            }));
        }

        let plan = PhysicalPlan::HilbertPartition(Box::new(HilbertPartition {
            plan_id: 0,
            input: plan,
            table_info: table_info.clone(),
            num_partitions: total_partitions,
        }));
        Ok(Some(Self::add_commit_sink(
            plan,
            is_distributed,
            table_info,
            snapshot,
            false,
            Some(recluster_info),
        )))
    }

    async fn build_linear_plan(
        &self,
        tbl: &Arc<dyn Table>,
        push_downs: &mut Option<PushDownInfo>,
        limit: Option<usize>,
    ) -> Result<Option<PhysicalPlan>> {
        let Some((parts, snapshot)) = tbl
            .recluster(self.ctx.clone(), push_downs.clone(), limit)
            .await?
        else {
            return Ok(None);
        };
        if parts.is_empty() {
            return Ok(None);
        }

        let table_info = tbl.get_table_info().clone();
        let is_distributed = parts.is_distributed(self.ctx.clone());
        let plan = match parts {
            ReclusterParts::Recluster {
                tasks,
                remained_blocks,
                removed_segment_indexes,
                removed_segment_summary,
            } => {
                let root = PhysicalPlan::Recluster(Box::new(Recluster {
                    tasks,
                    table_info: table_info.clone(),
                    plan_id: u32::MAX,
                }));

                Self::add_commit_sink(
                    root,
                    is_distributed,
                    table_info,
                    snapshot,
                    false,
                    Some(ReclusterInfoSideCar {
                        merged_blocks: remained_blocks,
                        removed_segment_indexes,
                        removed_statistics: removed_segment_summary,
                    }),
                )
            }
            ReclusterParts::Compact(parts) => {
                let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
                let root = PhysicalPlan::CompactSource(Box::new(CompactSource {
                    parts,
                    table_info: table_info.clone(),
                    column_ids: snapshot.schema.to_leaf_column_id_set(),
                    plan_id: u32::MAX,
                }));

                Self::add_commit_sink(root, is_distributed, table_info, snapshot, merge_meta, None)
            }
        };
        Ok(Some(plan))
    }

    fn build_push_downs(
        &self,
        push_downs: &mut Option<PushDownInfo>,
        tbl: &Arc<dyn Table>,
    ) -> Result<()> {
        if push_downs.is_none() {
            if let Some(expr) = &self.plan.selection {
                let settings = self.ctx.get_settings();
                let (mut bind_context, metadata) = bind_table(tbl.clone())?;
                let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                let mut type_checker = TypeChecker::try_create(
                    &mut bind_context,
                    self.ctx.clone(),
                    &name_resolution_ctx,
                    metadata,
                    &[],
                    true,
                )?;
                let (scalar, _) = *type_checker.resolve(expr)?;
                // prepare the filter expression
                let filter = cast_expr_to_non_null_boolean(
                    scalar
                        .as_expr()?
                        .project_column_ref(|col| col.column_name.clone()),
                )?;
                // prepare the inverse filter expression
                let inverted_filter =
                    check_function(None, "not", &[], &[filter.clone()], &BUILTIN_FUNCTIONS)?;
                *push_downs = Some(PushDownInfo {
                    filters: Some(Filters {
                        filter: filter.as_remote_expr(),
                        inverted_filter: inverted_filter.as_remote_expr(),
                    }),
                    ..PushDownInfo::default()
                });
            }
        }
        Ok(())
    }

    async fn build_hilbert_info(
        &self,
        tbl: &Arc<dyn Table>,
        hilbert_info: &mut Option<HilbertBuildInfo>,
    ) -> Result<()> {
        if hilbert_info.is_some() {
            return Ok(());
        }

        let database = &self.plan.database;
        let table = &self.plan.table;
        let settings = self.ctx.get_settings();
        let sample_size = settings.get_hilbert_sample_size_per_block()?;

        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let (mut expr_bind_context, expr_metadata) = bind_table(tbl.clone())?;
        let mut type_checker = TypeChecker::try_create(
            &mut expr_bind_context,
            self.ctx.clone(),
            &name_resolution_ctx,
            expr_metadata,
            &[],
            true,
        )?;

        let ast_exprs = tbl.resolve_cluster_keys(self.ctx.clone()).unwrap();
        let cluster_keys_len = ast_exprs.len();
        let mut cluster_key_strs = Vec::with_capacity(cluster_keys_len);
        let mut cluster_key_types = Vec::with_capacity(cluster_keys_len);
        for mut ast in ast_exprs {
            let mut normalizer = IdentifierNormalizer {
                ctx: &name_resolution_ctx,
            };
            ast.drive_mut(&mut normalizer);
            cluster_key_strs.push(format!("{:#}", &ast));

            let (scalar, _) = *type_checker.resolve(&ast)?;
            cluster_key_types.push(scalar.data_type()?);
        }

        let mut keys_bounds = Vec::with_capacity(cluster_key_strs.len());
        let mut hilbert_keys = Vec::with_capacity(cluster_key_strs.len());
        for cluster_key_str in cluster_key_strs.into_iter() {
            keys_bounds.push(format!(
                "range_bound(1000, {sample_size})({cluster_key_str})"
            ));

            hilbert_keys.push(format!(
                "hilbert_key(cast(range_partition_id({table}.{cluster_key_str}, []) as uint16))"
            ));
        }
        let hilbert_keys_str = hilbert_keys.join(", ");

        let keys_bounds_query =
            format!("SELECT {} FROM {database}.{table}", keys_bounds.join(", "));
        let keys_bound =
            plan_hilbert_sql(self.ctx.clone(), MetadataRef::default(), &keys_bounds_query).await?;

        let index_bound_query = format!(
            "SELECT \
                range_bound(1000, {sample_size})(hilbert_index([{hilbert_keys_str}], 2)) \
            FROM {database}.{table}"
        );
        let index_bound =
            plan_hilbert_sql(self.ctx.clone(), MetadataRef::default(), &index_bound_query).await?;

        let quote = settings.get_sql_dialect()?.default_ident_quote();
        let schema = tbl.schema_with_stream();
        let mut output_with_table = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            output_with_table.push(format!(
                "{quote}{table}{quote}.{quote}{}{quote}",
                field.name
            ));
        }
        let output_with_table_str = output_with_table.join(", ");
        let query = format!(
            "SELECT \
                {output_with_table_str}, \
                range_partition_id(hilbert_index([{hilbert_keys_str}], 2), [])AS _predicate \
            FROM {database}.{table}"
        );
        let query = plan_hilbert_sql(self.ctx.clone(), MetadataRef::default(), &query).await?;

        *hilbert_info = Some(HilbertBuildInfo {
            keys_bound,
            index_bound,
            query,
        });
        Ok(())
    }

    async fn execute_hilbert_plan(
        &self,
        executor: &Arc<ServiceQueryExecutor>,
        plan: &Plan,
        partitions: usize,
        variables: &VecDeque<Scalar>,
        tbl: &Arc<dyn Table>,
    ) -> Result<DataBlock> {
        let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } = plan
        else {
            unreachable!()
        };

        let s_expr = replace_with_constant(s_expr, variables, partitions as u16);
        metadata.write().replace_all_tables(tbl.clone());
        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
        let plan = builder.build(&s_expr, bind_context.column_set()).await?;
        let data_blocks = executor.execute_query_with_physical_plan(&plan).await?;
        DataBlock::concat(&data_blocks)
    }

    fn add_commit_sink(
        input: PhysicalPlan,
        is_distributed: bool,
        table_info: TableInfo,
        snapshot: Arc<TableSnapshot>,
        merge_meta: bool,
        recluster_info: Option<ReclusterInfoSideCar>,
    ) -> PhysicalPlan {
        let plan = if is_distributed {
            PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(input),
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            })
        } else {
            input
        };

        let kind = if recluster_info.is_some() {
            MutationKind::Recluster
        } else {
            MutationKind::Compact
        };
        PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(plan),
            table_info,
            snapshot: Some(snapshot),
            commit_type: CommitType::Mutation { kind, merge_meta },
            update_stream_meta: vec![],
            deduplicated_label: None,
            plan_id: u32::MAX,
            recluster_info,
        }))
    }
}

struct HilbertBuildInfo {
    keys_bound: Plan,
    index_bound: Plan,
    query: Plan,
}
