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

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use databend_common_ast::ast::Engine;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_schema_type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::core::always_callback;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_sql::Symbol;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_storages_basic::RecursiveCteMemoryTable;
use databend_storages_common_table_meta::table::OPT_KEY_RECURSIVE_CTE;
use futures_util::TryStreamExt;
use md5::Digest;
use md5::Md5;

use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::QueryFinishHooks;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanVisitor;
use crate::physical_plans::RecursiveCteScan;
use crate::physical_plans::UnionAll;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::stream::PullingExecutorStream;

// The whole recursive cte as source.
enum CachedReplayAction {
    Replay(VecDeque<DataBlock>),
    Populate,
}

pub struct TransformRecursiveCteSource {
    ctx: Arc<QueryContext>,
    union_plan: UnionAll,
    left_outputs: Vec<(Symbol, Option<Expr>)>,
    right_outputs: Vec<(Symbol, Option<Expr>)>,

    recursive_step: usize,
    cte_scan_tables: Vec<Arc<dyn Table>>,
    active_step_blocks: Option<VecDeque<DataBlock>>,
    active_step_has_output: bool,
    replay_blocks: Option<VecDeque<DataBlock>>,
    owns_cache_population: bool,
}

impl TransformRecursiveCteSource {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        union_plan: UnionAll,
    ) -> Result<ProcessorPtr> {
        let mut union_plan = union_plan;

        // Recursive CTE uses internal MEMORY tables addressed by name in the current database.
        // Use a stable per-query/per-plan prefix so duplicated recursive sources produced by
        // decorrelation can reuse the same internal tables and cached outputs.
        let rcte_prefix = make_rcte_prefix(&ctx, &union_plan)?;
        let local_cte_scan_names = {
            let names = collect_local_recursive_scan_names(
                &union_plan.right,
                union_plan.logical_recursive_cte_id,
            );
            if names.is_empty() {
                union_plan.cte_scan_names.clone()
            } else {
                names
            }
        };
        if union_plan.cte_scan_names != local_cte_scan_names {
            union_plan.cte_scan_names = local_cte_scan_names;
        }
        let local_cte_scan_name_set: HashSet<&str> = union_plan
            .cte_scan_names
            .iter()
            .map(String::as_str)
            .collect();

        rewrite_assign_and_strip_recursive_cte(
            &mut union_plan.left,
            union_plan.logical_recursive_cte_id,
            &local_cte_scan_name_set,
            &rcte_prefix,
        );
        rewrite_assign_and_strip_recursive_cte(
            &mut union_plan.right,
            union_plan.logical_recursive_cte_id,
            &local_cte_scan_name_set,
            &rcte_prefix,
        );
        for name in union_plan.cte_scan_names.iter_mut() {
            *name = format!("{rcte_prefix}{name}");
        }

        let left_outputs = union_plan
            .left_outputs
            .iter()
            .map(|(idx, remote_expr)| {
                if let Some(remote_expr) = remote_expr {
                    (*idx, Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS)))
                } else {
                    (*idx, None)
                }
            })
            .collect::<Vec<_>>();
        let right_outputs = union_plan
            .right_outputs
            .iter()
            .map(|(idx, remote_expr)| {
                if let Some(remote_expr) = remote_expr {
                    (*idx, Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS)))
                } else {
                    (*idx, None)
                }
            })
            .collect::<Vec<_>>();
        AsyncSourcer::create(
            ctx.get_scan_progress(),
            output_port,
            TransformRecursiveCteSource {
                ctx,
                union_plan,
                left_outputs,
                right_outputs,
                recursive_step: 0,
                cte_scan_tables: vec![],
                active_step_blocks: None,
                active_step_has_output: false,
                replay_blocks: None,
                owns_cache_population: false,
            },
        )
    }

    async fn execute_r_cte(
        ctx: Arc<QueryContext>,
        recursive_step: usize,
        union_plan: UnionAll,
    ) -> Result<(Vec<DataBlock>, Vec<Arc<dyn Table>>)> {
        if ctx.get_settings().get_max_cte_recursive_depth()? < recursive_step {
            return Err(ErrorCode::Internal("Recursive depth is reached"));
        }
        #[cfg(debug_assertions)]
        crate::test_kits::rcte_hooks::maybe_pause_before_step(&ctx.get_id(), recursive_step).await;
        let mut cte_scan_tables = vec![];
        let plan = if recursive_step == 0 {
            // Find all cte scan in the union right child plan, then create memory table for them.
            create_memory_table_for_cte_scan(&ctx, &union_plan.right).await?;
            // Cache cte scan tables, avoid to get them from catalog every time.
            for table_name in union_plan.cte_scan_names.iter() {
                let table = ctx
                    .get_table(
                        &ctx.get_current_catalog(),
                        &ctx.get_current_database(),
                        table_name,
                    )
                    .await?;
                cte_scan_tables.push(table);
            }
            union_plan.left.clone()
        } else {
            union_plan.right.clone()
        };
        ctx.clear_runtime_filter();
        let mut build_res = build_query_pipeline_without_render_result_set(&ctx, &plan).await?;
        build_res.main_pipeline.set_on_finished(always_callback(
            QueryFinishHooks::nested().into_callback(ctx.clone()),
        ));
        let settings = ExecutorSettings::try_create(ctx.clone())?;
        let pulling_executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
        ctx.set_executor(pulling_executor.get_inner())?;
        let isolate_runtime = Runtime::with_worker_threads(2, Some("r-cte-source".to_string()))?;
        let join_handle = isolate_runtime.spawn(async move {
            let stream = PullingExecutorStream::create(pulling_executor)?;
            stream.try_collect::<Vec<DataBlock>>().await
        });
        let data_blocks = join_handle.await??;
        Ok((data_blocks, cte_scan_tables))
    }
}

fn make_rcte_prefix(ctx: &Arc<QueryContext>, union_plan: &UnionAll) -> Result<String> {
    let query_id = ctx.get_id();
    let suffix = if query_id.is_empty() {
        "unknown"
    } else {
        query_id.as_str()
    };
    if let Some(logical_recursive_cte_id) = union_plan.logical_recursive_cte_id {
        let runtime_id =
            ctx.get_or_create_logical_recursive_cte_runtime_id(logical_recursive_cte_id);
        return Ok(format!(
            "__rcte_{suffix}_{runtime_id}_{logical_recursive_cte_id}_"
        ));
    }
    let mut names = union_plan.cte_scan_names.clone();
    names.sort();
    let digest = format!("{:x}", Md5::digest(names.join(",").as_bytes()));
    Ok(format!("__rcte_{suffix}_{digest}_"))
}

fn same_logical_recursive_cte(left: Option<u32>, right: Option<u32>) -> bool {
    left.zip(right).is_some_and(|(left, right)| left == right)
}

fn matches_logical_recursive_cte_scope(scope: Option<u32>, candidate: Option<u32>) -> bool {
    scope.is_none() || same_logical_recursive_cte(scope, candidate)
}

fn rewrite_assign_and_strip_recursive_cte(
    plan: &mut PhysicalPlan,
    logical_recursive_cte_id: Option<u32>,
    local_cte_scan_name_set: &HashSet<&str>,
    prefix: &str,
) {
    // Only nested recursive UNION nodes that reference the current recursive CTE should be
    // downgraded to normal unions to avoid nested recursive sources for the same table.
    if let Some(union_all) = UnionAll::from_mut_physical_plan(plan) {
        let same_logical_recursive_cte = same_logical_recursive_cte(
            logical_recursive_cte_id,
            union_all.logical_recursive_cte_id,
        );
        if !union_all.cte_scan_names.is_empty()
            && (same_logical_recursive_cte
                || union_all
                    .cte_scan_names
                    .iter()
                    .all(|name| local_cte_scan_name_set.contains(name.as_str())))
        {
            union_all.cte_scan_names.clear();
        }
    }

    if let Some(recursive_cte_scan) = RecursiveCteScan::from_mut_physical_plan(plan) {
        let same_logical_recursive_cte = same_logical_recursive_cte(
            logical_recursive_cte_id,
            recursive_cte_scan.logical_recursive_cte_id,
        );
        if same_logical_recursive_cte
            || local_cte_scan_name_set.contains(recursive_cte_scan.table_name.as_str())
        {
            recursive_cte_scan.table_name = format!("{prefix}{}", recursive_cte_scan.table_name);
        }
    }

    for child in plan.children_mut() {
        rewrite_assign_and_strip_recursive_cte(
            child,
            logical_recursive_cte_id,
            local_cte_scan_name_set,
            prefix,
        );
    }
}

fn collect_local_recursive_scan_names(
    plan: &PhysicalPlan,
    logical_recursive_cte_id: Option<u32>,
) -> Vec<String> {
    fn walk(
        plan: &PhysicalPlan,
        logical_recursive_cte_id: Option<u32>,
        names: &mut Vec<String>,
        seen: &mut HashSet<String>,
    ) {
        if let Some(union_all) = UnionAll::from_physical_plan(plan) {
            let same_logical_recursive_cte = same_logical_recursive_cte(
                logical_recursive_cte_id,
                union_all.logical_recursive_cte_id,
            );
            if !union_all.cte_scan_names.is_empty() && !same_logical_recursive_cte {
                return;
            }
        }

        if let Some(recursive_cte_scan) = RecursiveCteScan::from_physical_plan(plan) {
            let same_logical_recursive_cte = matches_logical_recursive_cte_scope(
                logical_recursive_cte_id,
                recursive_cte_scan.logical_recursive_cte_id,
            );
            if same_logical_recursive_cte && seen.insert(recursive_cte_scan.table_name.clone()) {
                names.push(recursive_cte_scan.table_name.clone());
            }
            return;
        }

        for child in plan.children() {
            walk(child, logical_recursive_cte_id, names, seen);
        }
    }

    let mut names = Vec::new();
    let mut seen = HashSet::new();
    walk(plan, logical_recursive_cte_id, &mut names, &mut seen);
    names
}

fn recursive_cte_memory_table(table: &dyn Table) -> &RecursiveCteMemoryTable {
    table
        .as_any()
        .downcast_ref::<RecursiveCteMemoryTable>()
        .unwrap()
}

fn first_recursive_cte_memory_table(tables: &[Arc<dyn Table>]) -> Option<&RecursiveCteMemoryTable> {
    tables
        .first()
        .map(|table| recursive_cte_memory_table(table.as_ref()))
}

fn finish_cache_population_for_tables(tables: &[Arc<dyn Table>], sealed: bool) {
    for table in tables {
        recursive_cte_memory_table(table.as_ref()).finish_cache_population(sealed);
    }
}

async fn prepare_cached_replay_action_for_tables(tables: &[Arc<dyn Table>]) -> CachedReplayAction {
    let Some(first_table) = first_recursive_cte_memory_table(tables) else {
        return CachedReplayAction::Populate;
    };

    loop {
        if first_table.is_sealed() {
            return CachedReplayAction::Replay(VecDeque::from(first_table.cached_output_blocks()));
        }

        if first_table.begin_cache_population() {
            for table in tables.iter().skip(1) {
                let memory_table = recursive_cte_memory_table(table.as_ref());
                memory_table.finish_cache_population(false);
                let started = memory_table.begin_cache_population();
                debug_assert!(
                    started,
                    "recursive CTE cache population should reset every scan table together"
                );
            }
            return CachedReplayAction::Populate;
        }

        while first_table.is_running() && !first_table.is_sealed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl TransformRecursiveCteSource {
    fn finish_owned_cache_population(&mut self, sealed: bool) {
        if self.owns_cache_population {
            finish_cache_population_for_tables(&self.cte_scan_tables, sealed);
            self.owns_cache_population = false;
        }
    }

    async fn try_prepare_cached_replay(&mut self) -> Result<bool> {
        create_memory_table_for_cte_scan(&self.ctx, &self.union_plan.right).await?;
        if self.cte_scan_tables.is_empty() {
            for table_name in self.union_plan.cte_scan_names.iter() {
                let table = self
                    .ctx
                    .get_table(
                        &self.ctx.get_current_catalog(),
                        &self.ctx.get_current_database(),
                        table_name,
                    )
                    .await?;
                self.cte_scan_tables.push(table);
            }
        }

        if self.cte_scan_tables.is_empty() {
            return Ok(false);
        }
        match prepare_cached_replay_action_for_tables(&self.cte_scan_tables).await {
            CachedReplayAction::Replay(replay_blocks) => {
                self.replay_blocks = Some(replay_blocks);
                Ok(true)
            }
            CachedReplayAction::Populate => {
                self.owns_cache_population = true;
                Ok(false)
            }
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for TransformRecursiveCteSource {
    const NAME: &'static str = "TransformRecursiveCteSource";

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(replay_blocks) = self.replay_blocks.as_mut() {
                if let Some(block) = replay_blocks.pop_front() {
                    return Ok(Some(block));
                }
                self.replay_blocks = None;
                return Ok(None);
            }

            if self.active_step_blocks.is_none() {
                if self.recursive_step == 0 && self.try_prepare_cached_replay().await? {
                    continue;
                }

                if self.recursive_step > 0 {
                    for table in self.cte_scan_tables.iter() {
                        let memory_table = recursive_cte_memory_table(table.as_ref());
                        memory_table.set_active_generation(self.recursive_step - 1);
                    }
                }

                match Self::execute_r_cte(
                    self.ctx.clone(),
                    self.recursive_step,
                    self.union_plan.clone(),
                )
                .await
                {
                    Ok((data_blocks, cte_scan_tables)) => {
                        if !cte_scan_tables.is_empty() {
                            self.cte_scan_tables = cte_scan_tables;
                        }
                        if data_blocks.is_empty() {
                            self.finish_owned_cache_population(true);
                            return Ok(None);
                        }
                        self.active_step_blocks = Some(VecDeque::from(data_blocks));
                        self.active_step_has_output = false;
                    }
                    Err(e) => {
                        self.finish_owned_cache_population(false);
                        return Err(ErrorCode::Internal(format!(
                            "failed to start recursive cte step: {:?}",
                            e
                        )));
                    }
                }
            }

            if let Some(data) = self
                .active_step_blocks
                .as_mut()
                .and_then(|blocks| blocks.pop_front())
            {
                let func_ctx = self.ctx.get_function_context()?;
                let projected_block = project_block(
                    &func_ctx,
                    data,
                    &self.union_plan.left.output_schema()?,
                    &self.union_plan.right.output_schema()?,
                    &self.left_outputs,
                    &self.right_outputs,
                    self.recursive_step == 0,
                )?;

                if projected_block.num_rows() == 0 {
                    continue;
                }

                for table in self.cte_scan_tables.iter() {
                    let memory_table = recursive_cte_memory_table(table.as_ref());
                    memory_table
                        .append_generation_block(self.recursive_step, projected_block.clone());
                }

                if let Some(first_table) = first_recursive_cte_memory_table(&self.cte_scan_tables) {
                    first_table.cache_output_block(projected_block.clone());
                }

                self.active_step_has_output = true;
                return Ok(Some(projected_block));
            }

            self.active_step_blocks = None;
            if self.active_step_has_output {
                self.recursive_step += 1;
                continue;
            }

            self.finish_owned_cache_population(true);
            return Ok(None);
        }
    }

    async fn on_finish(&mut self) -> Result<()> {
        self.finish_owned_cache_population(false);
        Ok(())
    }
}

async fn create_memory_table_for_cte_scan(
    ctx: &Arc<QueryContext>,
    plan: &PhysicalPlan,
) -> Result<()> {
    struct CollectMemoryTable {
        ctx: Arc<QueryContext>,
        plans: Vec<CreateTablePlan>,
    }

    impl CollectMemoryTable {
        pub fn create(ctx: Arc<QueryContext>) -> Box<dyn PhysicalPlanVisitor> {
            Box::new(CollectMemoryTable { ctx, plans: vec![] })
        }

        pub fn take(&mut self) -> Vec<CreateTablePlan> {
            std::mem::take(&mut self.plans)
        }
    }

    impl PhysicalPlanVisitor for CollectMemoryTable {
        fn as_any(&mut self) -> &mut dyn Any {
            self
        }

        fn visit(&mut self, plan: &PhysicalPlan) -> Result<()> {
            if let Some(recursive_cte_scan) = RecursiveCteScan::from_physical_plan(plan) {
                let table_fields = recursive_cte_scan
                    .output_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        Ok(TableField::new(
                            field.name(),
                            infer_schema_type(field.data_type())?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let schema = TableSchemaRefExt::create(table_fields);

                let mut options = BTreeMap::new();
                options.insert(OPT_KEY_RECURSIVE_CTE.to_string(), "1".to_string());
                self.plans.push(CreateTablePlan {
                    schema,
                    create_option: CreateOption::CreateIfNotExists,
                    tenant: Tenant {
                        tenant: self.ctx.get_tenant().tenant,
                    },
                    catalog: self.ctx.get_current_catalog(),
                    database: self.ctx.get_current_database(),
                    table: recursive_cte_scan.table_name.clone(),
                    engine: Engine::Memory,
                    engine_options: Default::default(),
                    table_properties: Default::default(),
                    table_partition: None,
                    storage_params: None,
                    options,
                    field_comments: vec![],
                    cluster_key: None,
                    as_select: None,
                    table_indexes: None,
                    table_constraints: None,
                    attached_columns: None,
                });
            }

            Ok(())
        }
    }

    let mut visitor = CollectMemoryTable::create(ctx.clone());
    plan.visit(&mut visitor)?;

    let create_table_plans = {
        let visitor = visitor
            .as_any()
            .downcast_mut::<CollectMemoryTable>()
            .unwrap();
        visitor.take()
    };

    for create_table_plan in create_table_plans {
        let table_name = create_table_plan.table.clone();
        let database_name = create_table_plan.database.clone();
        let create_table_interpreter =
            CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
        create_table_interpreter.execute2().await?;
        ctx.add_m_cte_temp_table(&database_name, &table_name);
    }

    Ok(())
}

fn project_block(
    func_ctx: &FunctionContext,
    block: DataBlock,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
    left_outputs: &[(Symbol, Option<Expr>)],
    right_outputs: &[(Symbol, Option<Expr>)],
    is_left: bool,
) -> Result<DataBlock> {
    let num_rows = block.num_rows();
    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let columns = left_outputs
        .iter()
        .zip(right_outputs.iter())
        .map(|(left, right)| {
            if is_left {
                if let Some(expr) = &left.1 {
                    let entry = BlockEntry::new(evaluator.run(expr)?, || {
                        (expr.data_type().clone(), num_rows)
                    });
                    Ok(entry)
                } else {
                    Ok(block
                        .get_by_offset(left_schema.index_of(&left.0.to_string())?)
                        .clone())
                }
            } else if let Some(expr) = &right.1 {
                let entry = BlockEntry::new(evaluator.run(expr)?, || {
                    (expr.data_type().clone(), num_rows)
                });
                Ok(entry)
            } else if left.1.is_some() {
                Ok(block
                    .get_by_offset(right_schema.index_of(&right.0.to_string())?)
                    .clone())
            } else {
                check_type(
                    &left.0.to_string(),
                    &right.0.to_string(),
                    &block,
                    left_schema,
                    right_schema,
                )
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DataBlock::new(columns, num_rows))
}

fn check_type(
    left_name: &str,
    right_name: &str,
    block: &DataBlock,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
) -> Result<BlockEntry> {
    let left_field = left_schema.field_with_name(left_name)?;
    let left_data_type = left_field.data_type();

    let right_field = right_schema.field_with_name(right_name)?;
    let right_data_type = right_field.data_type();

    let index = right_schema.index_of(right_name)?;

    if left_data_type == right_data_type {
        return Ok(block.get_by_offset(index).clone());
    }

    if left_data_type.remove_nullable() == right_data_type.remove_nullable() {
        let origin = block.get_by_offset(index).clone();
        let mut builder = ColumnBuilder::with_capacity(left_data_type, block.num_rows());
        for idx in 0..block.num_rows() {
            let scalar = origin.index(idx).unwrap();
            builder.push(scalar);
        }
        Ok(builder.build().into())
    } else {
        Err(ErrorCode::IllegalDataType(
            "The data type on both sides of the union does not match",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use databend_common_catalog::table::Table;
    use databend_common_meta_app::schema::TableInfo;
    use databend_common_sql::plans::TableOptions;
    use databend_common_storages_basic::RecursiveCteMemoryTable;
    use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;

    use super::*;

    static NEXT_TEST_TABLE_ID: AtomicU64 = AtomicU64::new(1);

    fn new_test_recursive_cte_memory_table() -> Arc<dyn Table> {
        let table_id = NEXT_TEST_TABLE_ID.fetch_add(1, Ordering::Relaxed);
        let mut table_info = TableInfo::default();
        table_info.ident.table_id = table_id;
        table_info.meta.options = TableOptions::default();
        table_info.meta.options.insert(
            OPT_KEY_TEMP_PREFIX.to_string(),
            format!("rcte-test-{table_id}"),
        );

        Arc::from(RecursiveCteMemoryTable::try_create(table_info).unwrap())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cached_replay_reacquires_population_after_unsealed_exit() {
        let table = new_test_recursive_cte_memory_table();
        let memory_table = recursive_cte_memory_table(table.as_ref());
        assert!(memory_table.begin_cache_population());

        let waiter_table = table.clone();
        let waiter = databend_common_base::runtime::spawn(async move {
            prepare_cached_replay_action_for_tables(&[waiter_table]).await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        memory_table.finish_cache_population(false);

        match waiter.await.unwrap() {
            CachedReplayAction::Populate => {}
            CachedReplayAction::Replay(_) => {
                panic!("expected cache ownership to be reacquired after unsealed exit")
            }
        }

        assert!(memory_table.is_running());
        assert!(!memory_table.is_sealed());
        memory_table.finish_cache_population(false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cached_replay_reacquires_population_for_all_scan_tables() {
        let first_table = new_test_recursive_cte_memory_table();
        let second_table = new_test_recursive_cte_memory_table();
        let first_memory_table = recursive_cte_memory_table(first_table.as_ref());
        let second_memory_table = recursive_cte_memory_table(second_table.as_ref());

        second_memory_table.append_generation_block(0, DataBlock::empty());
        second_memory_table.set_active_generation(0);
        let stale_reader = second_memory_table.register_reader();
        assert!(second_memory_table.take_one_block(stale_reader).is_some());

        assert!(first_memory_table.begin_cache_population());

        let waiter_first_table = first_table.clone();
        let waiter_second_table = second_table.clone();
        let waiter = databend_common_base::runtime::spawn(async move {
            prepare_cached_replay_action_for_tables(&[waiter_first_table, waiter_second_table])
                .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        first_memory_table.finish_cache_population(false);

        match waiter.await.unwrap() {
            CachedReplayAction::Populate => {}
            CachedReplayAction::Replay(_) => {
                panic!("expected cache ownership to be reacquired for every scan table")
            }
        }

        assert!(first_memory_table.is_running());
        assert!(second_memory_table.is_running());
        assert!(!first_memory_table.is_sealed());
        assert!(!second_memory_table.is_sealed());
        assert!(second_memory_table.take_one_block(stale_reader).is_none());

        first_memory_table.finish_cache_population(false);
        second_memory_table.finish_cache_population(false);
    }
}
