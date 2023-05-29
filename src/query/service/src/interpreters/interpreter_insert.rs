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
use std::io::BufRead;
use std::io::Cursor;
use std::ops::Not;
use std::str::FromStr;
use std::sync::Arc;

use aho_corasick::AhoCorasick;
use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_base::runtime::GlobalIORuntime;
use common_catalog::table::AppendMode;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_formats::FastFieldDecoderValues;
use common_io::cursor_ext::ReadBytesExt;
use common_io::cursor_ext::ReadCheckPointExt;
use common_meta_app::principal::StageFileFormatType;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_sql::executor::DistributedInsertSelect;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::plans::Insert;
use common_sql::plans::InsertInputSource;
use common_sql::plans::Plan;
use common_sql::BindContext;
use common_sql::Metadata;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::interpreters::common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::processors::transforms::TransformRuntimeCastSchema;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::SourcePipeBuilder;
use crate::schedulers::build_query_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InsertInterpreter {
    ctx: Arc<QueryContext>,
    plan: Insert,
    source_pipe_builder: Mutex<Option<SourcePipeBuilder>>,
}

impl InsertInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Insert) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreter {
            ctx,
            plan,
            source_pipe_builder: Mutex::new(None),
        }))
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
        let cast_needed = select_schema != DataSchema::from(output_schema.as_ref()).into();
        Ok(cast_needed)
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        let mut build_res = PipelineBuildResult::create();

        match &self.plan.source {
            InsertInputSource::Stage(_) => {
                unreachable!()
            }
            InsertInputSource::Values(data) => {
                let settings = self.ctx.get_settings();

                build_res.main_pipeline.add_source(
                    |output| {
                        let name_resolution_ctx =
                            NameResolutionContext::try_from(settings.as_ref())?;
                        let inner = ValueSource::new(
                            data.to_string(),
                            self.ctx.clone(),
                            name_resolution_ctx,
                            plan.schema(),
                        );
                        AsyncSourcer::create(self.ctx.clone(), output, inner)
                    },
                    1,
                )?;
            }
            InsertInputSource::StreamingWithFormat(format, _, input_context) => {
                let input_context = input_context.as_ref().expect("must success").clone();
                input_context
                    .format
                    .exec_stream(input_context.clone(), &mut build_res.main_pipeline)?;

                match StageFileFormatType::from_str(format) {
                    Ok(f) if f.has_inner_schema() => {
                        let dest_schema = plan.schema();
                        let func_ctx = self.ctx.get_function_context()?;

                        build_res.main_pipeline.add_transform(
                            |transform_input_port, transform_output_port| {
                                TransformRuntimeCastSchema::try_create(
                                    transform_input_port,
                                    transform_output_port,
                                    dest_schema.clone(),
                                    func_ctx.clone(),
                                )
                            },
                        )?;
                    }
                    _ => {}
                }
            }
            InsertInputSource::StreamingWithFileFormat(params, _, input_context) => {
                let input_context = input_context.as_ref().expect("must success").clone();
                input_context
                    .format
                    .exec_stream(input_context.clone(), &mut build_res.main_pipeline)?;

                if params.get_type().has_inner_schema() {
                    let dest_schema = plan.schema();
                    let func_ctx = self.ctx.get_function_context()?;

                    build_res.main_pipeline.add_transform(
                        |transform_input_port, transform_output_port| {
                            TransformRuntimeCastSchema::try_create(
                                transform_input_port,
                                transform_output_port,
                                dest_schema.clone(),
                                func_ctx.clone(),
                            )
                        },
                    )?;
                }
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
                        let mut builder1 =
                            PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone());
                        (builder1.build(s_expr).await?, bind_context.columns.clone())
                    }
                    _ => unreachable!(),
                };

                let catalog = self.plan.catalog.clone();
                let insert_select_plan = match select_plan {
                    PhysicalPlan::Exchange(ref mut exchange) => {
                        // insert can be dispatched to different nodes
                        let input = exchange.input.clone();
                        exchange.input = Box::new(PhysicalPlan::DistributedInsertSelect(Box::new(
                            DistributedInsertSelect {
                                input,
                                catalog,
                                table_info: table1.get_table_info().clone(),
                                select_schema: plan.schema(),
                                select_column_bindings,
                                insert_schema: self.plan.schema(),
                                cast_needed: self.check_schema_cast(plan)?,
                            },
                        )));
                        select_plan
                    }
                    other_plan => {
                        // insert should wait until all nodes finished
                        PhysicalPlan::DistributedInsertSelect(Box::new(DistributedInsertSelect {
                            input: Box::new(other_plan),
                            catalog,
                            table_info: table1.get_table_info().clone(),
                            select_schema: plan.schema(),
                            select_column_bindings,
                            insert_schema: self.plan.schema(),
                            cast_needed: self.check_schema_cast(plan)?,
                        }))
                    }
                };

                let mut build_res =
                    build_query_pipeline(&self.ctx, &[], &insert_select_plan, false, false).await?;

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
                        return GlobalIORuntime::instance().block_on(async move {
                            // TODO doc this
                            let copied_files = None;
                            table
                                .commit_insertion(ctx, append_entries, copied_files, overwrite)
                                .await
                        });
                    }

                    Err(may_error.as_ref().unwrap().clone())
                });

                return Ok(build_res);
            }
        };

        let append_mode = match &self.plan.source {
            InsertInputSource::StreamingWithFormat(..)
            | InsertInputSource::StreamingWithFileFormat(..) => AppendMode::Copy,
            _ => AppendMode::Normal,
        };

        append2table(
            self.ctx.clone(),
            table.clone(),
            plan.schema(),
            &mut build_res,
            self.plan.overwrite,
            true,
            append_mode,
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

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        // Pre-generate the positions of `(`, `'` and `\`
        let patterns = &["(", "'", "\\"];
        let ac = AhoCorasick::new(patterns);
        // Use the number of '(' to estimate the number of rows
        let mut estimated_rows = 0;
        let mut positions = VecDeque::new();
        for mat in ac.find_iter(&self.data) {
            if mat.pattern() == 0 {
                estimated_rows += 1;
                continue;
            }
            positions.push_back(mat.start());
        }

        let mut reader = Cursor::new(self.data.as_bytes());
        let block = self
            .read(estimated_rows, &mut reader, &mut positions)
            .await?;
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

    #[async_backtrace::framed]
    pub async fn read<R: AsRef<[u8]>>(
        &self,
        estimated_rows: usize,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<DataBlock> {
        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::with_capacity(f.data_type(), estimated_rows))
            .collect::<Vec<_>>();

        let mut bind_context = self.bind_context.clone();

        let format = self.ctx.get_format_settings()?;
        let field_decoder = FastFieldDecoderValues::create_for_insert(format);

        for row in 0.. {
            let _ = reader.ignore_white_spaces();
            if reader.eof() {
                break;
            }
            // Not the first row
            if row != 0 {
                reader.must_ignore_byte(b',')?;
            }

            self.parse_next_row(
                &field_decoder,
                reader,
                &mut columns,
                positions,
                &mut bind_context,
                self.metadata.clone(),
            )
            .await?;
        }

        let columns = columns
            .into_iter()
            .map(|col| col.build())
            .collect::<Vec<_>>();
        Ok(DataBlock::new_from_columns(columns))
    }

    /// Parse single row value, like ('111', 222, 1 + 1)
    #[async_backtrace::framed]
    async fn parse_next_row<R: AsRef<[u8]>>(
        &self,
        field_decoder: &FastFieldDecoderValues,
        reader: &mut Cursor<R>,
        columns: &mut [ColumnBuilder],
        positions: &mut VecDeque<usize>,
        bind_context: &mut BindContext,
        metadata: MetadataRef,
    ) -> Result<()> {
        let _ = reader.ignore_white_spaces();
        let col_size = columns.len();
        let start_pos_of_row = reader.checkpoint();

        // Start of the row --- '('
        if !reader.ignore_byte(b'(') {
            return Err(ErrorCode::BadDataValueType(
                "Must start with parentheses".to_string(),
            ));
        }
        // Ignore the positions in the previous row.
        while let Some(pos) = positions.front() {
            if *pos < start_pos_of_row as usize {
                positions.pop_front();
            } else {
                break;
            }
        }

        for col_idx in 0..col_size {
            let _ = reader.ignore_white_spaces();
            let col_end = if col_idx + 1 == col_size { b')' } else { b',' };

            let col = columns
                .get_mut(col_idx)
                .ok_or_else(|| ErrorCode::Internal("ColumnBuilder is None"))?;

            let (need_fallback, pop_count) = field_decoder
                .read_field(col, reader, positions)
                .map(|_| {
                    let _ = reader.ignore_white_spaces();
                    let need_fallback = reader.ignore_byte(col_end).not();
                    (need_fallback, col_idx + 1)
                })
                .unwrap_or((true, col_idx));

            // ColumnBuilder and expr-parser both will eat the end ')' of the row.
            if need_fallback {
                for col in columns.iter_mut().take(pop_count) {
                    col.pop();
                }
                // rollback to start position of the row
                reader.rollback(start_pos_of_row + 1);
                skip_to_next_row(reader, 1)?;
                let end_pos_of_row = reader.position();

                // Parse from expression and append all columns.
                reader.set_position(start_pos_of_row);
                let row_len = end_pos_of_row - start_pos_of_row;
                let buf = &reader.remaining_slice()[..row_len as usize];

                let sql = std::str::from_utf8(buf).unwrap();
                let settings = self.ctx.get_settings();
                let sql_dialect = settings.get_sql_dialect()?;
                let tokens = tokenize_sql(sql)?;
                let exprs = parse_comma_separated_exprs(&tokens[1..tokens.len()], sql_dialect)?;

                let values = bind_context
                    .exprs_to_scalar(
                        exprs,
                        &self.schema,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        metadata,
                    )
                    .await?;

                for (col, scalar) in columns.iter_mut().zip(values) {
                    col.push(scalar.as_ref());
                }
                reader.set_position(end_pos_of_row);
                return Ok(());
            }
        }

        Ok(())
    }
}

// Values |(xxx), (yyy), (zzz)
pub fn skip_to_next_row<R: AsRef<[u8]>>(reader: &mut Cursor<R>, mut balance: i32) -> Result<()> {
    let _ = reader.ignore_white_spaces();

    let mut quoted = false;
    let mut escaped = false;

    while balance > 0 {
        let buffer = reader.remaining_slice();
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
