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
use std::time::Duration;
use std::time::SystemTime;

use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::bind_table;
use databend_common_sql::executor::physical_plans::create_push_down_filters;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::CompactSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::HilbertPartition;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::Recluster;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::set_update_stream_columns;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::ReclusterPlan;
use databend_common_sql::query_executor::QueryExecutor;
use databend_common_sql::IdentifierNormalizer;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::Planner;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeChecker;
use databend_enterprise_hilbert_clustering::get_hilbert_clustering_handler;
use databend_storages_common_table_meta::table::ClusterType;
use derive_visitor::DriveMut;
use itertools::Itertools;
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

            let res = self.execute_recluster(&mut push_downs).await;

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

            self.ctx.clear_selected_segment_locations();
            self.ctx.evict_table_from_cache(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.table,
            )?;
        }

        Ok(PipelineBuildResult::create())
    }
}

impl ReclusterTableInterpreter {
    async fn execute_recluster(&self, push_downs: &mut Option<PushDownInfo>) -> Result<bool> {
        let start = SystemTime::now();

        let ReclusterPlan {
            catalog,
            database,
            table,
            selection,
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
        let table_info = tbl.get_table_info().clone();
        // check mutability
        tbl.check_mutable()?;
        let Some(cluster_type) = tbl.cluster_type() else {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table '{}.{}'",
                database, table,
            )));
        };

        if push_downs.is_none() {
            if let Some(expr) = &selection {
                let (mut bind_context, metadata) = bind_table(tbl.clone())?;
                let name_resolution_ctx =
                    NameResolutionContext::try_from(self.ctx.get_settings().as_ref())?;
                let mut type_checker = TypeChecker::try_create(
                    &mut bind_context,
                    self.ctx.clone(),
                    &name_resolution_ctx,
                    metadata,
                    &[],
                    true,
                )?;
                let (scalar, _) = *type_checker.resolve(expr)?;
                let filters = create_push_down_filters(&scalar)?;
                *push_downs = Some(PushDownInfo {
                    filters: Some(filters),
                    ..PushDownInfo::default()
                });
            }
        }

        let mut physical_plan = match cluster_type {
            ClusterType::Hilbert => {
                LicenseManagerSwitch::instance().check_enterprise_enabled(
                    self.ctx.get_license_key(),
                    Feature::HilbertClustering,
                )?;
                let handler = get_hilbert_clustering_handler();
                let Some((recluster_info, snapshot)) = handler
                    .do_hilbert_clustering(tbl.clone(), self.ctx.clone(), push_downs.clone())
                    .await?
                else {
                    return Ok(true);
                };

                let block_thresholds = tbl.get_block_thresholds();
                let total_bytes = recluster_info.removed_statistics.uncompressed_byte_size as usize;
                let total_rows = recluster_info.removed_statistics.row_count as usize;
                let rows_per_block = block_thresholds.calc_rows_per_block(total_bytes, total_rows);
                let total_partitions = std::cmp::max(total_rows / rows_per_block, 1);

                let ast_exprs = tbl.resolve_cluster_keys(self.ctx.clone()).unwrap();
                let cluster_keys_len = ast_exprs.len();
                let settings = self.ctx.get_settings();
                let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                let cluster_key_strs = ast_exprs.into_iter().fold(
                    Vec::with_capacity(cluster_keys_len),
                    |mut acc, mut ast| {
                        let mut normalizer = IdentifierNormalizer {
                            ctx: &name_resolution_ctx,
                        };
                        ast.drive_mut(&mut normalizer);
                        acc.push(format!("{:#}", &ast));
                        acc
                    },
                );

                let query_str = self.ctx.get_query_str();
                let write_progress = self.ctx.get_write_progress();
                let write_progress_value = write_progress.as_ref().get_values();

                let subquery_executor = Arc::new(ServiceQueryExecutor::new(
                    QueryContext::create_from(self.ctx.as_ref()),
                ));
                let partitions = settings.get_hilbert_num_range_ids()?;
                let sample_size = settings.get_hilbert_sample_size_per_block()?;
                let mut keys_bounds = Vec::with_capacity(cluster_key_strs.len());
                for (index, cluster_key_str) in cluster_key_strs.iter().enumerate() {
                    keys_bounds.push(format!(
                        "range_bound({partitions}, {sample_size})({cluster_key_str}) AS bound_{index}"
                    ));
                }
                let keys_bounds_query =
                    format!("SELECT {} FROM {database}.{table}", keys_bounds.join(", "));
                let data_blocks = subquery_executor
                    .execute_query_with_sql_string(&keys_bounds_query)
                    .await?;
                let keys_bounds = DataBlock::concat(&data_blocks)?;

                let mut hilbert_keys = Vec::with_capacity(keys_bounds.num_columns());
                for (entry, cluster_key_str) in keys_bounds
                    .columns()
                    .iter()
                    .zip(cluster_key_strs.into_iter())
                {
                    let v = entry.value.index(0).unwrap().to_string();
                    hilbert_keys.push(format!(
                        "hilbert_key(cast(range_partition_id({table}.{cluster_key_str}, {v}) as uint16))"
                    ));
                }
                let hilbert_keys_str = hilbert_keys.join(", ");
                let index_bound_query = format!(
                    "WITH _source_data AS ( \
                        SELECT \
                            hilbert_index([{hilbert_keys_str}], 2) AS index \
                        FROM {database}.{table} \
                    ) \
                    SELECT range_bound({total_partitions}, {sample_size})(index) AS bound \
                        FROM _source_data"
                );
                let data_blocks = subquery_executor
                    .execute_query_with_sql_string(&index_bound_query)
                    .await?;
                debug_assert!(data_blocks.len() == 1);
                let val = data_blocks[0].value_at(0, 0).unwrap();
                let col = val.as_array().unwrap().as_binary().unwrap();
                let index_bound_str = col
                    .iter()
                    .map(|s| {
                        let binary = s
                            .iter()
                            .map(|byte| format!("{:02X}", byte))
                            .collect::<Vec<String>>()
                            .join("");
                        format!("unhex('{}')", binary)
                    })
                    .join(", ");

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
                        range_partition_id(hilbert_index([{hilbert_keys_str}], 2), [{index_bound_str}])AS _predicate \
                    FROM {database}.{table}"
                );
                let tokens = tokenize_sql(query.as_str())?;
                let sql_dialect = self
                    .ctx
                    .get_settings()
                    .get_sql_dialect()
                    .unwrap_or_default();
                let (stmt, _) = parse_sql(&tokens, sql_dialect)?;

                let mut planner = Planner::new(self.ctx.clone());
                let plan = planner.plan_stmt(&stmt, false).await?;
                let Plan::Query {
                    mut s_expr,
                    metadata,
                    bind_context,
                    ..
                } = plan
                else {
                    unreachable!()
                };
                if tbl.change_tracking_enabled() {
                    *s_expr = set_update_stream_columns(&s_expr)?;
                }

                write_progress.set(&write_progress_value);
                self.ctx.attach_query_str(QueryKind::Other, query_str);
                let mut builder = PhysicalPlanBuilder::new(metadata, self.ctx.clone(), false);
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

                let mut plan = PhysicalPlan::HilbertPartition(Box::new(HilbertPartition {
                    plan_id: 0,
                    input: plan,
                    table_info: table_info.clone(),
                    num_partitions: total_partitions,
                    rows_per_block,
                }));

                if is_distributed {
                    plan = PhysicalPlan::Exchange(Exchange {
                        plan_id: 0,
                        input: Box::new(plan),
                        kind: FragmentKind::Merge,
                        keys: vec![],
                        allow_adjust_parallelism: true,
                        ignore_exchange: false,
                    });
                }

                PhysicalPlan::CommitSink(Box::new(CommitSink {
                    input: Box::new(plan),
                    table_info,
                    snapshot: Some(snapshot),
                    mutation_kind: MutationKind::Recluster,
                    update_stream_meta: vec![],
                    merge_meta: false,
                    deduplicated_label: None,
                    plan_id: u32::MAX,
                    recluster_info: Some(recluster_info),
                }))
            }
            ClusterType::Linear => {
                let Some((parts, snapshot)) = tbl
                    .recluster(self.ctx.clone(), push_downs.clone(), *limit)
                    .await?
                else {
                    return Ok(true);
                };
                if parts.is_empty() {
                    return Ok(true);
                }

                let is_distributed = parts.is_distributed(self.ctx.clone());
                match parts {
                    ReclusterParts::Recluster {
                        tasks,
                        remained_blocks,
                        removed_segment_indexes,
                        removed_segment_summary,
                    } => {
                        let mut root = PhysicalPlan::Recluster(Box::new(Recluster {
                            tasks,
                            table_info: table_info.clone(),
                            plan_id: u32::MAX,
                        }));

                        if is_distributed {
                            root = PhysicalPlan::Exchange(Exchange {
                                plan_id: 0,
                                input: Box::new(root),
                                kind: FragmentKind::Merge,
                                keys: vec![],
                                allow_adjust_parallelism: true,
                                ignore_exchange: false,
                            });
                        }
                        PhysicalPlan::CommitSink(Box::new(CommitSink {
                            input: Box::new(root),
                            table_info,
                            snapshot: Some(snapshot),
                            mutation_kind: MutationKind::Recluster,
                            update_stream_meta: vec![],
                            merge_meta: false,
                            deduplicated_label: None,
                            plan_id: u32::MAX,
                            recluster_info: Some(ReclusterInfoSideCar {
                                merged_blocks: remained_blocks,
                                removed_segment_indexes,
                                removed_statistics: removed_segment_summary,
                            }),
                        }))
                    }
                    ReclusterParts::Compact(parts) => {
                        let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
                        let mut root = PhysicalPlan::CompactSource(Box::new(CompactSource {
                            parts,
                            table_info: table_info.clone(),
                            column_ids: snapshot.schema.to_leaf_column_id_set(),
                            plan_id: u32::MAX,
                        }));

                        if is_distributed {
                            root = PhysicalPlan::Exchange(Exchange {
                                plan_id: 0,
                                input: Box::new(root),
                                kind: FragmentKind::Merge,
                                keys: vec![],
                                allow_adjust_parallelism: true,
                                ignore_exchange: false,
                            });
                        }

                        PhysicalPlan::CommitSink(Box::new(CommitSink {
                            input: Box::new(root),
                            table_info,
                            snapshot: Some(snapshot),
                            mutation_kind: MutationKind::Compact,
                            update_stream_meta: vec![],
                            merge_meta,
                            deduplicated_label: None,
                            plan_id: u32::MAX,
                            recluster_info: None,
                        }))
                    }
                }
            }
        };
        physical_plan.adjust_plan_id(&mut 0);
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        debug_assert!(build_res.main_pipeline.is_complete_pipeline()?);

        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        build_res.set_max_threads(max_threads);

        let executor_settings = ExecutorSettings::try_create(self.ctx.clone())?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor =
            PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
        self.ctx.clear_written_segment_locations()?;
        self.ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;
        // make sure the executor is dropped before the next loop.
        drop(complete_executor);
        // make sure the lock guard is dropped before the next loop.
        drop(lock_guard);

        // vacuum temp files.
        self.ctx.unload_spill_meta();
        hook_clear_m_cte_temp_table(&self.ctx)?;
        hook_vacuum_temp_files(&self.ctx)?;
        hook_disk_temp_dir(&self.ctx)?;

        InterpreterClusteringHistory::write_log(&self.ctx, start, database, table)?;
        Ok(false)
    }
}
