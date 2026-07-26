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

use chrono::Duration;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContextCluster;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::Constraint;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline_transforms::TransformPipelineHelper;
#[cfg(feature = "storage-stage")]
use databend_common_pipeline_transforms::columns::TransformAddConstColumns;
use databend_common_sql::ColumnBinding;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::binder::ConstraintExprBinder;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::Insert;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::InsertValue;
use databend_common_sql::plans::Plan;
use databend_common_storages_fuse::operations::TransformConstraintVerify;
use databend_common_storages_paimon::PAIMON_ENGINE;
use databend_common_storages_paimon::PaimonTable;
#[cfg(feature = "storage-stage")]
use databend_query_storage_stage_support::build_streaming_load_pipeline;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use log::info;

use crate::clusters::ClusterHelper;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::physical_plans::ConstantTableScan;
use crate::physical_plans::DistributedInsertSelect;
use crate::physical_plans::Exchange;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PAIMON_ROUTE_KEY_NAME;
use crate::physical_plans::PaimonWritePrepare;
use crate::physical_plans::PaimonWriteRoute;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanMeta;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::RawValueSource;
use crate::pipelines::ValueSource;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextProgress;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;
use crate::stream::DataBlockStream;

fn is_fixed_bucket_primary_key(table: &Arc<dyn Table>) -> Result<bool> {
    match PaimonTable::try_from_table(table.as_ref()) {
        Ok(paimon) => Ok(paimon.is_fixed_bucket_primary_key()),
        Err(_) => Ok(false),
    }
}

fn wrap_paimon_write_distribution(
    input: PhysicalPlan,
    table: Arc<dyn Table>,
) -> Result<PhysicalPlan> {
    if table.get_table_info().meta.engine != PAIMON_ENGINE || !is_fixed_bucket_primary_key(&table)?
    {
        return Ok(input);
    }

    let routed = PhysicalPlan::new(PaimonWriteRoute::try_create(
        input,
        table.get_table_info().clone(),
    )?);
    let key = RemoteExpr::ColumnRef {
        span: None,
        id: routed.output_schema()?.num_fields() - 1,
        data_type: DataType::Binary,
        display_name: PAIMON_ROUTE_KEY_NAME.to_string(),
    };
    Ok(PhysicalPlan::new(Exchange {
        input: routed,
        kind: FragmentKind::GlobalShuffle,
        keys: vec![key],
        allow_adjust_parallelism: false,
        ignore_exchange: false,
        meta: PhysicalPlanMeta::new("Exchange"),
    }))
}

/// Build the INSERT SELECT physical plan (including Paimon route/exchange when needed).
pub fn build_insert_select_physical_plan(
    select_plan: PhysicalPlan,
    select_schema: DataSchemaRef,
    select_column_bindings: Vec<ColumnBinding>,
    insert_schema: DataSchemaRef,
    table: Arc<dyn Table>,
    cast_needed: bool,
    table_meta_timestamps: TableMetaTimestamps,
    distributed: bool,
) -> Result<PhysicalPlan> {
    let table_info = table.get_table_info().clone();
    let needs_pk_route = is_fixed_bucket_primary_key(&table)?;

    let prepare_and_route = |input: PhysicalPlan| -> Result<PhysicalPlan> {
        let prepared = PhysicalPlan::new(PaimonWritePrepare {
            meta: PhysicalPlanMeta::new("PaimonWritePrepare"),
            input,
            table_info: table_info.clone(),
            insert_schema: insert_schema.clone(),
            select_schema: select_schema.clone(),
            select_column_bindings: select_column_bindings.clone(),
            cast_needed,
        });
        wrap_paimon_write_distribution(prepared, table.clone())
    };

    if table.support_distributed_insert()
        && let Some(exchange) = Exchange::from_physical_plan(&select_plan)
    {
        let input = if needs_pk_route {
            prepare_and_route(exchange.input.clone())?
        } else {
            exchange.input.clone()
        };
        let insert = PhysicalPlan::new(DistributedInsertSelect {
            input,
            table_info,
            select_schema,
            select_column_bindings,
            insert_schema,
            cast_needed,
            input_prepared: needs_pk_route,
            table_meta_timestamps,
            meta: PhysicalPlanMeta::new("DistributedInsertSelect"),
        });
        // Keep the outer Merge in a cluster so writer metas gather on the coordinator.
        // A single node has no self flight edge, so the local writer pipeline is already
        // the coordinator pipeline and must not retain that Merge.
        if needs_pk_route && !distributed {
            Ok(insert)
        } else {
            Ok(exchange.derive(vec![insert]))
        }
    } else {
        let input = if needs_pk_route {
            prepare_and_route(select_plan)?
        } else {
            select_plan
        };
        let insert = PhysicalPlan::new(DistributedInsertSelect {
            input,
            table_info,
            select_schema,
            select_column_bindings,
            insert_schema,
            cast_needed,
            input_prepared: needs_pk_route,
            table_meta_timestamps,
            meta: PhysicalPlanMeta::new("DistributedInsertSelect"),
        });
        // VALUES / local select has no top Merge; synthesize one for PK so commit metas gather.
        if needs_pk_route && distributed {
            Ok(PhysicalPlan::new(Exchange {
                input: insert,
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
                meta: PhysicalPlanMeta::new("Exchange"),
            }))
        } else {
            Ok(insert)
        }
    }
}

fn values_to_constant_scan(
    rows: &[Vec<databend_common_expression::Scalar>],
    schema: DataSchemaRef,
) -> Result<(PhysicalPlan, Vec<ColumnBinding>)> {
    let num_rows = rows.len();
    let num_fields = schema.num_fields();
    let mut values = Vec::with_capacity(num_fields);
    let mut bindings = Vec::with_capacity(num_fields);
    let mut output_fields = Vec::with_capacity(num_fields);

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let data_type = field.data_type();
        let mut builder = ColumnBuilder::with_capacity(data_type, num_rows);
        for row in rows {
            builder.push(row[col_idx].as_ref());
        }
        values.push(builder.build());
        output_fields.push(DataField::new(&col_idx.to_string(), data_type.clone()));
        bindings.push(
            ColumnBindingBuilder::new(
                field.name().clone(),
                Symbol::from_field_index(col_idx),
                Box::new(data_type.clone()),
                Visibility::Visible,
            )
            .build(),
        );
    }

    let output_schema = DataSchemaRefExt::create(output_fields);
    let plan = PhysicalPlan::new(ConstantTableScan {
        values,
        num_rows,
        output_schema,
        meta: PhysicalPlanMeta::new("ConstantTableScan"),
    });
    Ok((plan, bindings))
}

pub struct InsertInterpreter {
    ctx: Arc<QueryContext>,
    plan: Insert,
}

impl InsertInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Insert) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreter { ctx, plan }))
    }

    fn check_schema_cast(&self, plan: &Plan) -> Result<bool> {
        let output_schema = &self.plan.schema;
        let select_schema = plan.schema();

        // validate schema
        if select_schema.fields().len() != output_schema.fields().len() {
            return Err(ErrorCode::BadArguments(format!(
                "Fields in select statement is not equal with expected, select fields: {}, insert fields: {}",
                select_schema.fields().len(),
                output_schema.fields().len(),
            )));
        }

        // check if cast needed
        let cast_needed = select_schema.as_ref() != &DataSchema::from(output_schema.as_ref());
        Ok(cast_needed)
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }
        let table = if let Some(table_info) = &self.plan.table_info {
            // if table_info is provided, we should instantiated table with it.
            self.ctx
                .get_catalog(&self.plan.catalog)
                .await?
                .get_table_by_info(table_info)?
        } else {
            self.ctx
                .get_table_with_branch(
                    &self.plan.catalog,
                    &self.plan.database,
                    &self.plan.table,
                    self.plan.branch.as_deref(),
                )
                .await?
        };

        let mut table_constraints = Vec::new();
        // check mutability
        table.check_mutable()?;
        let table_meta_timestamps = if table.engine() == "FUSE" {
            let fuse_table =
                databend_common_storages_fuse::FuseTable::try_from_table(table.as_ref())?;

            // bind constraints
            let dest_schema = self.plan.dest_schema();
            let mut expr_binder = ConstraintExprBinder::try_new(self.ctx.clone(), dest_schema)?;
            for (constraint_name, constraint) in fuse_table.get_table_info().meta.constraints.iter()
            {
                match &constraint {
                    Constraint::Check(expr) => {
                        let constraint_expr = expr_binder.get_expr(constraint_name, expr)?;
                        table_constraints.push((constraint_name.clone(), constraint_expr))
                    }
                }
            }

            let snapshot = fuse_table.read_table_snapshot().await?;
            self.ctx
                .get_table_meta_timestamps(table.as_ref(), snapshot)?
        } else {
            // For non-fuse table, the table meta timestamps does not matter,
            // just passes a placeholder value here
            TableMetaTimestamps::new(None, Duration::hours(1))
        };

        let mut build_res = PipelineBuildResult::create();

        match &self.plan.source {
            InsertInputSource::Stage(_) => {
                unreachable!()
            }
            InsertInputSource::Values(InsertValue::Values { rows }) => {
                // Fixed-bucket PK Paimon tables need route + GlobalHash Exchange; unify with
                // the SELECT insert physical plan so single-node uses local channel repartition.
                if is_fixed_bucket_primary_key(&table)? {
                    let insert_schema = self.plan.dest_schema();
                    let (values_plan, bindings) =
                        values_to_constant_scan(rows, insert_schema.clone())?;
                    let select_schema = values_plan.output_schema()?;
                    let mut insert_plan = build_insert_select_physical_plan(
                        values_plan,
                        select_schema,
                        bindings,
                        insert_schema,
                        table.clone(),
                        false,
                        table_meta_timestamps,
                        self.ctx.get_cluster().get_nodes().len() > 1,
                    )?;
                    insert_plan.adjust_plan_id(&mut 0);
                    let mut build_res =
                        build_query_pipeline_without_render_result_set(&self.ctx, &insert_plan)
                            .await?;

                    table.commit_insertion(
                        self.ctx.clone(),
                        &mut build_res.main_pipeline,
                        None,
                        vec![],
                        self.plan.overwrite,
                        None,
                        unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                        table_meta_timestamps,
                    )?;

                    if self.plan.branch.is_none() {
                        let hook_operator = HookOperator::create(
                            self.ctx.clone(),
                            self.plan.catalog.clone(),
                            self.plan.database.clone(),
                            self.plan.table.clone(),
                            MutationKind::Insert,
                            LockTableOption::LockNoRetry,
                        );
                        hook_operator.execute(&mut build_res.main_pipeline).await;
                    }

                    return Ok(build_res);
                }

                build_res.main_pipeline.add_source(
                    |output| {
                        let inner = ValueSource::new(rows.clone(), self.plan.dest_schema());
                        AsyncSourcer::create(self.ctx.get_scan_progress(), output, inner)
                    },
                    1,
                )?;
            }
            InsertInputSource::Values(InsertValue::RawValues { data, start }) => {
                build_res.main_pipeline.add_source(
                    |output| {
                        let name_resolution_ctx = NameResolutionContext {
                            deny_column_reference: true,
                            ..Default::default()
                        };
                        let inner = RawValueSource::new(
                            data.to_string(),
                            self.ctx.clone(),
                            name_resolution_ctx,
                            self.plan.dest_schema(),
                            *start,
                        );
                        AsyncSourcer::create(self.ctx.get_scan_progress(), output, inner)
                    },
                    1,
                )?;
            }
            InsertInputSource::SelectPlan(plan) => {
                let table1 = table.clone();
                let (select_plan, select_column_bindings, metadata) = match plan.as_ref() {
                    Plan::Query {
                        s_expr,
                        metadata,
                        bind_context,
                        ..
                    } => {
                        let mut builder1 =
                            PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                        (
                            builder1.build(s_expr, bind_context.column_set()).await?,
                            bind_context.columns.clone(),
                            metadata,
                        )
                    }
                    _ => unreachable!(),
                };

                let explain_plan = {
                    let metadata = metadata.read();
                    select_plan
                        .format(&metadata, Default::default())?
                        .format_pretty()?
                };

                info!("Insert select plan: \n{}", explain_plan);

                let update_stream_meta = dml_build_update_stream_req(self.ctx.clone()).await?;

                let mut insert_select_plan = build_insert_select_physical_plan(
                    select_plan,
                    plan.schema(),
                    select_column_bindings,
                    self.plan.dest_schema(),
                    table1,
                    self.check_schema_cast(plan)?,
                    table_meta_timestamps,
                    self.ctx.get_cluster().get_nodes().len() > 1,
                )?;

                insert_select_plan.adjust_plan_id(&mut 0);
                let mut build_res =
                    build_query_pipeline_without_render_result_set(&self.ctx, &insert_select_plan)
                        .await?;

                table.commit_insertion(
                    self.ctx.clone(),
                    &mut build_res.main_pipeline,
                    None,
                    update_stream_meta,
                    self.plan.overwrite,
                    None,
                    unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                    table_meta_timestamps,
                )?;

                //  Execute the hook operator.
                if self.plan.branch.is_none() {
                    let hook_operator = HookOperator::create(
                        self.ctx.clone(),
                        self.plan.catalog.clone(),
                        self.plan.database.clone(),
                        self.plan.table.clone(),
                        MutationKind::Insert,
                        LockTableOption::LockNoRetry,
                    );
                    hook_operator.execute(&mut build_res.main_pipeline).await;
                }

                return Ok(build_res);
            }
            #[cfg(feature = "storage-stage")]
            InsertInputSource::StreamingLoad(plan) => {
                build_streaming_load_pipeline(
                    self.ctx.clone(),
                    &mut build_res.main_pipeline,
                    &plan.file_format,
                    plan.receiver.clone(),
                    plan.required_source_schema.clone(),
                    plan.default_exprs.clone(),
                    plan.block_thresholds,
                )?;
                if !plan.values_consts.is_empty() {
                    let input_schema = Arc::new(DataSchema::from(&plan.required_source_schema));
                    build_res.main_pipeline.try_add_transformer(|| {
                        let ctx = self.ctx.get_function_context()?;
                        TransformAddConstColumns::try_new(
                            ctx,
                            input_schema.clone(),
                            plan.required_values_schema.clone(),
                            plan.values_consts.clone(),
                        )
                    })?;
                }
            }
            #[cfg(not(feature = "storage-stage"))]
            InsertInputSource::StreamingLoad(_) => {
                return Err(ErrorCode::Unimplemented(
                    "Streaming load support is disabled, rebuild with cargo feature 'storage-stage'",
                ));
            }
        };
        if !table_constraints.is_empty() {
            build_res
                .main_pipeline
                .try_add_async_accumulating_transformer(|| {
                    Ok(TransformConstraintVerify::new(
                        table_constraints.clone(),
                        self.ctx.get_function_context()?,
                        table.name().to_string(),
                    ))
                })?;
        }

        PipelineBuilder::build_append2table_with_commit_pipeline(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            table.clone(),
            self.plan.dest_schema(),
            None,
            vec![],
            self.plan.overwrite,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            table_meta_timestamps,
        )?;

        //  Execute the hook operator.
        if self.plan.branch.is_none() {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                self.plan.catalog.clone(),
                self.plan.database.clone(),
                self.plan.table.clone(),
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let binding = self.ctx.mutation_state().mutation_status();
        let status = binding.read().unwrap();
        let blocks = vec![DataBlock::new_from_columns(vec![UInt64Type::from_data(
            vec![status.insert_rows],
        )])];
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}
