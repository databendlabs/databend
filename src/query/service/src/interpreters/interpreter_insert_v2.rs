// Copyright 2022 Datafuse Labs.
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

use std::io::Cursor;
use std::ops::Not;
use std::sync::Arc;

use common_ast::ast::Expr;
use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_base::base::GlobalIORuntime;
use common_base::base::TrySpawn;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::BufferRead;
use common_io::prelude::BufferReadExt;
use common_io::prelude::BufferReader;
use common_io::prelude::NestedCheckpointReader;
use common_pipeline_sources::processors::sources::AsyncSource;
use common_pipeline_sources::processors::sources::AsyncSourcer;
use common_pipeline_transforms::processors::transforms::Transform;
use common_planner::Metadata;
use common_planner::MetadataRef;
use parking_lot::Mutex;
use parking_lot::RwLock;

use super::interpreter_common::append2table;
use super::plan_schedulers::build_schedule_pipeline;
use crate::evaluator::EvalNode;
use crate::evaluator::Evaluator;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::transforms::ExpressionTransformV2;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::binder::ScalarBinder;
use crate::sql::executor::DistributedInsertSelect;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::executor::PipelineBuilder;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::Insert;
use crate::sql::plans::InsertInputSource;
use crate::sql::plans::Plan;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;
use crate::sql::NameResolutionContext;

pub struct InsertInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: Insert,
    source_pipe_builder: Mutex<Option<SourcePipeBuilder>>,
    async_insert: bool,
}

impl InsertInterpreterV2 {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: Insert,
        async_insert: bool,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreterV2 {
            ctx,
            plan,
            source_pipe_builder: Mutex::new(None),
            async_insert,
        }))
    }

    fn check_schema_cast(&self, plan: &Plan) -> Result<bool> {
        let output_schema = &self.plan.schema;
        let select_schema = plan.schema();

        // validate schema
        if select_schema.fields().len() < output_schema.fields().len() {
            return Err(ErrorCode::BadArguments(
                "Fields in select statement is less than expected",
            ));
        }

        // check if cast needed
        let cast_needed = select_schema != *output_schema;
        Ok(cast_needed)
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertInterpreterV2 {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        let mut build_res = PipelineBuildResult::create();
        let mut builder = SourcePipeBuilder::create();

        if self.async_insert {
            build_res.main_pipeline.add_pipe(
                ((*self.source_pipe_builder.lock()).clone())
                    .ok_or_else(|| ErrorCode::EmptyData("empty source pipe builder"))?
                    .finalize(),
            );
        } else {
            match &self.plan.source {
                InsertInputSource::Values(data) => {
                    let output_port = OutputPort::create();
                    let settings = self.ctx.get_settings();
                    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                    let inner = ValueSource::new(
                        data.to_string(),
                        self.ctx.clone(),
                        name_resolution_ctx,
                        plan.schema(),
                    );
                    let source =
                        AsyncSourcer::create(self.ctx.clone(), output_port.clone(), inner)?;
                    builder.add_source(output_port, source);

                    build_res.main_pipeline.add_pipe(builder.finalize());
                }
                InsertInputSource::StreamingWithFormat(_, _, input_context) => {
                    let input_context = input_context.as_ref().expect("must success").clone();
                    input_context
                        .format
                        .exec_stream(input_context.clone(), &mut build_res.main_pipeline)?;
                }
                InsertInputSource::SelectPlan(plan) => {
                    let table1 = table.clone();
                    let (mut select_plan, select_column_bindings) = match plan.as_ref() {
                        Plan::Query {
                            s_expr,
                            metadata,
                            bind_context,
                            ..
                        } => {
                            let builder1 =
                                PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone());
                            (builder1.build(s_expr).await?, bind_context.columns.clone())
                        }
                        _ => unreachable!(),
                    };

                    table1.get_table_info();
                    let catalog = self.plan.catalog.clone();
                    let is_distributed_plan = select_plan.is_distributed_plan();

                    let insert_select_plan = match select_plan {
                        PhysicalPlan::Exchange(ref mut exchange) => {
                            // insert can be dispatched to different nodes
                            let input = exchange.input.clone();
                            exchange.input = Box::new(PhysicalPlan::DistributedInsertSelect(
                                Box::new(DistributedInsertSelect {
                                    input,
                                    catalog,
                                    table_info: table1.get_table_info().clone(),
                                    select_schema: plan.schema(),
                                    select_column_bindings,
                                    insert_schema: self.plan.schema(),
                                    cast_needed: self.check_schema_cast(plan)?,
                                }),
                            ));
                            select_plan
                        }
                        other_plan => {
                            // insert should wait until all nodes finished
                            PhysicalPlan::DistributedInsertSelect(Box::new(
                                DistributedInsertSelect {
                                    input: Box::new(other_plan),
                                    catalog,
                                    table_info: table1.get_table_info().clone(),
                                    select_schema: plan.schema(),
                                    select_column_bindings,
                                    insert_schema: self.plan.schema(),
                                    cast_needed: self.check_schema_cast(plan)?,
                                },
                            ))
                        }
                    };

                    let mut build_res = match is_distributed_plan {
                        true => {
                            build_schedule_pipeline(self.ctx.clone(), &insert_select_plan).await
                        }
                        false => {
                            PipelineBuilder::create(self.ctx.clone()).finalize(&insert_select_plan)
                        }
                    }?;

                    let ctx = self.ctx.clone();
                    let overwrite = self.plan.overwrite;
                    build_res.main_pipeline.set_on_finished(move |may_error| {
                        // capture out variable
                        let overwrite = overwrite;
                        let ctx = ctx.clone();
                        let table = table.clone();

                        if may_error.is_none() {
                            let append_entries = ctx.consume_precommit_blocks();
                            // We must put the commit operation to global runtime, which will avoid the "dispatch dropped without returning error" in tower
                            let commit_handle = GlobalIORuntime::instance().spawn(async move {
                                table.commit_insertion(ctx, append_entries, overwrite).await
                            });

                            return match futures::executor::block_on(commit_handle) {
                                Ok(Ok(_)) => Ok(()),
                                Ok(Err(error)) => Err(error),
                                Err(cause) => Err(ErrorCode::PanicError(format!(
                                    "Maybe panic while in commit insert. {}",
                                    cause
                                ))),
                            };
                        }

                        Err(may_error.as_ref().unwrap().clone())
                    });

                    return Ok(build_res);
                }
            };
        }

        append2table(
            self.ctx.clone(),
            table.clone(),
            plan.schema(),
            &mut build_res,
            self.plan.overwrite,
            true,
        )?;

        Ok(build_res)
    }

    fn set_source_pipe_builder(&self, builder: Option<SourcePipeBuilder>) -> Result<()> {
        let mut guard = self.source_pipe_builder.lock();
        *guard = builder;
        Ok(())
    }
}

pub struct ValueSource {
    data: String,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: NameResolutionContext,
    bind_context: BindContext,
    schema: DataSchemaRef,
    metadata: MetadataRef,
    is_finished: bool,
}

#[async_trait::async_trait]
impl AsyncSource for ValueSource {
    const NAME: &'static str = "ValueSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }
        let cursor = Cursor::new(self.data.as_bytes());
        let mut reader = NestedCheckpointReader::new(BufferReader::new(cursor));
        let block = self.read(&mut reader).await?;
        self.is_finished = true;
        Ok(Some(block))
    }
}

impl ValueSource {
    pub fn new(
        data: String,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: NameResolutionContext,
        schema: DataSchemaRef,
    ) -> Self {
        let bind_context = BindContext::new();
        let metadata = Arc::new(RwLock::new(Metadata::default()));

        Self {
            data,
            ctx,
            name_resolution_ctx,
            schema,
            bind_context,
            metadata,
            is_finished: false,
        }
    }

    pub async fn read<R: BufferRead>(
        &self,
        reader: &mut NestedCheckpointReader<R>,
    ) -> Result<DataBlock> {
        let mut desers = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_deserializer(1024))
            .collect::<Vec<_>>();

        let col_size = desers.len();
        let mut rows = 0;

        loop {
            let _ = reader.ignore_white_spaces()?;
            if !reader.has_data_left()? {
                break;
            }
            // Not the first row
            if rows != 0 {
                reader.must_ignore_byte(b',')?;
            }

            self.parse_next_row(
                reader,
                col_size,
                &mut desers,
                &self.bind_context,
                self.metadata.clone(),
            )
            .await?;
            rows += 1;
        }

        if rows == 0 {
            return Ok(DataBlock::empty_with_schema(self.schema.clone()));
        }

        let columns = desers
            .iter_mut()
            .map(|deser| deser.finish_to_column())
            .collect::<Vec<_>>();

        Ok(DataBlock::create(self.schema.clone(), columns))
    }

    /// Parse single row value, like ('111', 222, 1 + 1)
    async fn parse_next_row<R: BufferRead>(
        &self,
        reader: &mut NestedCheckpointReader<R>,
        col_size: usize,
        desers: &mut [TypeDeserializerImpl],
        bind_context: &BindContext,
        metadata: MetadataRef,
    ) -> Result<()> {
        let _ = reader.ignore_white_spaces()?;
        reader.push_checkpoint();

        // Start of the row --- '('
        if !reader.ignore_byte(b'(')? {
            return Err(ErrorCode::BadDataValueType(
                "Must start with parentheses".to_string(),
            ));
        }

        let format = self.ctx.get_format_settings()?;
        for col_idx in 0..col_size {
            let _ = reader.ignore_white_spaces()?;
            let col_end = if col_idx + 1 == col_size { b')' } else { b',' };

            let deser = desers
                .get_mut(col_idx)
                .ok_or_else(|| ErrorCode::BadBytes("Deserializer is None"))?;

            let (need_fallback, pop_count) = deser
                .de_text_quoted(reader, &format)
                .and_then(|_| {
                    let _ = reader.ignore_white_spaces()?;
                    let need_fallback = reader.ignore_byte(col_end)?.not();
                    Ok((need_fallback, col_idx + 1))
                })
                .unwrap_or((true, col_idx));

            // Deserializer and expr-parser both will eat the end ')' of the row.
            if need_fallback {
                for deser in desers.iter_mut().take(pop_count) {
                    deser.pop_data_value()?;
                }
                skip_to_next_row(reader, 1)?;

                // Parse from expression and append all columns.
                let buf = reader.get_checkpoint_buffer();

                let sql = std::str::from_utf8(buf).unwrap();
                let settings = self.ctx.get_settings();
                let sql_dialect = settings.get_sql_dialect()?;
                let tokens = tokenize_sql(sql)?;
                let backtrace = Backtrace::new();
                let exprs = parse_comma_separated_exprs(
                    &tokens[1..tokens.len() as usize],
                    sql_dialect,
                    &backtrace,
                )?;

                let values = exprs_to_datavalue(
                    exprs,
                    &self.schema,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    bind_context,
                    metadata,
                )
                .await?;

                reader.pop_checkpoint();

                for (append_idx, deser) in desers.iter_mut().enumerate().take(col_size) {
                    deser.append_data_value(values[append_idx].clone(), &format)?;
                }

                return Ok(());
            }
        }

        reader.pop_checkpoint();
        Ok(())
    }
}

// Values |(xxx), (yyy), (zzz)
pub fn skip_to_next_row<R: BufferRead>(
    reader: &mut NestedCheckpointReader<R>,
    mut balance: i32,
) -> Result<()> {
    let _ = reader.ignore_white_spaces()?;

    let mut quoted = false;
    let mut escaped = false;

    while balance > 0 {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            break;
        }

        let size = buffer.len();

        let it = buffer
            .iter()
            .position(|&c| c == b'(' || c == b')' || c == b'\\' || c == b'\'');

        if let Some(it) = it {
            let c = buffer[it];
            reader.consume(it + 1);

            if it == 0 && escaped {
                escaped = false;
                continue;
            }
            escaped = false;

            match c {
                b'\\' => {
                    escaped = true;
                    continue;
                }
                b'\'' => {
                    quoted ^= true;
                    continue;
                }
                b')' => {
                    if !quoted {
                        balance -= 1;
                    }
                }
                b'(' => {
                    if !quoted {
                        balance += 1;
                    }
                }
                _ => {}
            }
        } else {
            escaped = false;
            reader.consume(size);
        }
    }
    Ok(())
}

fn fill_default_value(expressions: &mut Vec<(EvalNode, String)>, field: &DataField) -> Result<()> {
    if let Some(default_expr) = field.default_expr() {
        expressions.push((
            Evaluator::eval_physical_scalar(&serde_json::from_str(default_expr)?)?,
            field.name().to_string(),
        ));
    } else {
        // If field data type is nullable, then we'll fill it with null.
        if field.data_type().is_nullable() {
            let scalar = Scalar::ConstantExpr(ConstantExpr {
                value: DataValue::Null,
                data_type: Box::new(field.data_type().clone()),
            });
            expressions.push((Evaluator::eval_scalar(&scalar)?, field.name().to_string()));
        } else {
            expressions.push((
                Evaluator::eval_scalar(&Scalar::ConstantExpr(ConstantExpr {
                    value: field.data_type().default_value(),
                    data_type: Box::new(field.data_type().clone()),
                }))?,
                field.name().to_string(),
            ));
        }
    }
    Ok(())
}

async fn exprs_to_datavalue<'a>(
    exprs: Vec<Expr<'a>>,
    schema: &DataSchemaRef,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &NameResolutionContext,
    bind_context: &BindContext,
    metadata: MetadataRef,
) -> Result<Vec<DataValue>> {
    let schema_fields_len = schema.fields().len();
    if exprs.len() > schema_fields_len {
        return Err(ErrorCode::LogicalError(
            "Column count shouldn't be more than the number of schema",
        ));
    }
    if exprs.len() < schema_fields_len {
        return Err(ErrorCode::LogicalError(
            "Column count doesn't match value count",
        ));
    }
    let mut expressions = Vec::with_capacity(schema_fields_len);
    for (i, expr) in exprs.iter().enumerate() {
        // `DEFAULT` in insert values will be parsed as `Expr::ColumnRef`.
        if let Expr::ColumnRef { column, .. } = expr {
            if column.name.eq_ignore_ascii_case("default") {
                let field = schema.field(i);
                fill_default_value(&mut expressions, field)?;
                continue;
            }
        }
        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            ctx.clone(),
            name_resolution_ctx,
            metadata.clone(),
            &[],
        );
        let (mut scalar, data_type) = scalar_binder.bind(expr).await?;
        let field_data_type = schema.field(i).data_type();
        if data_type.ne(field_data_type) {
            scalar = Scalar::CastExpr(CastExpr {
                argument: Box::new(scalar),
                from_type: Box::new(data_type),
                target_type: Box::new(field_data_type.clone()),
            })
        }
        expressions.push((
            Evaluator::eval_scalar(&scalar)?,
            schema.field(i).name().to_string(),
        ));
    }

    let dummy = DataSchemaRefExt::create(vec![DataField::new("dummy", u8::to_data_type())]);
    let one_row_block = DataBlock::create(dummy, vec![Series::from_data(vec![1u8])]);
    let func_ctx = ctx.try_get_function_context()?;
    let mut expression_transform = ExpressionTransformV2 {
        expressions,
        func_ctx,
    };
    let res = expression_transform.transform(one_row_block)?;
    let datavalues: Vec<DataValue> = res.columns().iter().skip(1).map(|col| col.get(0)).collect();
    Ok(datavalues)
}
