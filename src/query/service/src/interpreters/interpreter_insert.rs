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
use std::time::Instant;

use aho_corasick::AhoCorasick;
use common_ast::ast::Expr as AExpr;
use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::parse_expr;
use common_ast::parser::parser_values_with_placeholder;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_base::runtime::GlobalIORuntime;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_catalog::table_context::StageAttachment;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::types::number::NumberScalar;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::BlockEntry;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_expression::Scalar;
use common_expression::Value;
use common_formats::FastFieldDecoderValues;
use common_io::cursor_ext::ReadBytesExt;
use common_io::cursor_ext::ReadCheckPointExt;
use common_meta_app::principal::FileFormatOptionsAst;
use common_meta_app::principal::StageFileFormatType;
use common_meta_app::principal::StageInfo;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_pipeline_transforms::processors::transforms::Transform;
use common_sql::binder::wrap_cast;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_sql::executor::DistributedInsertSelect;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::plans::FunctionCall;
use common_sql::plans::Insert;
use common_sql::plans::InsertInputSource;
use common_sql::plans::Plan;
use common_sql::plans::ScalarExpr;
use common_sql::BindContext;
use common_sql::Metadata;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use common_sql::ScalarBinder;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use common_storages_factory::Table;
use common_storages_fuse::io::Files;
use common_storages_stage::StageTable;
use common_users::UserApiProvider;
use parking_lot::Mutex;
use parking_lot::RwLock;
use tracing::error;
use tracing::info;

use crate::interpreters::common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::processors::transforms::TransformAddConstColumns;
use crate::pipelines::processors::transforms::TransformRuntimeCastSchema;
use crate::pipelines::processors::TransformResortAddOn;
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

    #[async_backtrace::framed]
    async fn try_purge_files(
        ctx: Arc<QueryContext>,
        stage_info: &StageInfo,
        stage_files: &[StageFileInfo],
    ) {
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let op = StageTable::get_op(stage_info);
        match op {
            Ok(op) => {
                let file_op = Files::create(table_ctx, op);
                let files = stage_files
                    .iter()
                    .map(|v| v.path.clone())
                    .collect::<Vec<_>>();
                if let Err(e) = file_op.remove_file_in_batch(&files).await {
                    error!("Failed to delete file: {:?}, error: {}", files, e);
                }
            }
            Err(e) => {
                error!("Failed to get stage table op, error: {}", e);
            }
        }
    }

    #[async_backtrace::framed]
    async fn prepared_values(&self, values_str: &str) -> Result<(DataSchemaRef, Vec<Scalar>)> {
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let tokens = tokenize_sql(values_str)?;
        let expr_or_placeholders = parser_values_with_placeholder(&tokens, sql_dialect)?;
        let source_schema = self.plan.schema();

        if source_schema.num_fields() != expr_or_placeholders.len() {
            return Err(ErrorCode::SemanticError(format!(
                "need {} fields in values, got only {}",
                source_schema.num_fields(),
                expr_or_placeholders.len()
            )));
        }

        let mut attachment_fields = vec![];
        let mut const_fields = vec![];
        let mut exprs = vec![];
        for (i, eo) in expr_or_placeholders.into_iter().enumerate() {
            match eo {
                Some(e) => {
                    exprs.push(e);
                    const_fields.push(source_schema.fields()[i].clone());
                }
                None => attachment_fields.push(source_schema.fields()[i].clone()),
            }
        }
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let mut bind_context = BindContext::new();
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let const_schema = Arc::new(DataSchema::new(const_fields));
        let const_values = exprs_to_scalar(
            exprs,
            &const_schema,
            self.ctx.clone(),
            &name_resolution_ctx,
            &mut bind_context,
            metadata,
        )
        .await?;
        Ok((Arc::new(DataSchema::new(attachment_fields)), const_values))
    }

    #[async_backtrace::framed]
    async fn build_insert_from_stage_pipeline(
        &self,
        table: Arc<dyn Table>,
        attachment: Arc<StageAttachment>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let start = Instant::now();
        let ctx = self.ctx.clone();
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let source_schema = self.plan.schema();
        let catalog_name = self.plan.catalog.clone();
        let overwrite = self.plan.overwrite;

        let (attachment_data_schema, const_columns) = if attachment.values_str.is_empty() {
            (source_schema.clone(), vec![])
        } else {
            self.prepared_values(&attachment.values_str).await?
        };

        let (mut stage_info, path) = parse_stage_location(&self.ctx, &attachment.location).await?;

        if let Some(ref options) = attachment.file_format_options {
            stage_info.file_format_params = FileFormatOptionsAst {
                options: options.clone(),
            }
            .try_into()?;
        }
        if let Some(ref options) = attachment.copy_options {
            stage_info.copy_options.apply(options, true)?;
        }

        let attachment_table_schema = infer_table_schema(&attachment_data_schema)?;
        let mut stage_table_info = StageTableInfo {
            schema: attachment_table_schema,
            stage_info,
            files_info: StageFilesInfo {
                path: path.to_string(),
                files: None,
                pattern: None,
            },
            files_to_copy: None,
        };

        let all_source_files = StageTable::list_files(&stage_table_info, None).await?;

        info!(
            "insert: read all stage attachment files finished: {}, elapsed:{}",
            all_source_files.len(),
            start.elapsed().as_secs()
        );

        stage_table_info.files_to_copy = Some(all_source_files.clone());
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let read_source_plan = {
            stage_table
                .read_plan_with_catalog(ctx.clone(), catalog_name, None, None)
                .await?
        };

        stage_table.read_data(table_ctx, &read_source_plan, pipeline)?;

        if !const_columns.is_empty() {
            pipeline.add_transform(|transform_input_port, transform_output_port| {
                TransformAddConstColumns::try_create(
                    ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    attachment_data_schema.clone(),
                    source_schema.clone(),
                    const_columns.clone(),
                )
            })?;
        }

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformResortAddOn::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                table.clone(),
            )
        })?;

        table.append_data(ctx.clone(), pipeline, AppendMode::Copy, false)?;

        let stage_info_clone = stage_table_info.stage_info.clone();
        pipeline.set_on_finished(move |may_error| {
            // capture out variable
            let overwrite = overwrite;
            let ctx = ctx.clone();
            let table = table.clone();
            let stage_info = stage_info_clone.clone();
            let all_source_files = all_source_files.clone();

            match may_error {
                Some(error) => {
                    tracing::error!("insert stage file error: {}", error);
                    GlobalIORuntime::instance()
                        .block_on(async move {
                            if stage_info.copy_options.purge {
                                info!(
                                    "insert: try to purge files:{}, elapsed:{}",
                                    all_source_files.len(),
                                    start.elapsed().as_secs()
                                );
                                Self::try_purge_files(ctx.clone(), &stage_info, &all_source_files)
                                    .await;
                            }
                            Ok(())
                        })
                        .unwrap_or_else(|e| {
                            tracing::error!("insert: purge stage file error: {}", e);
                        });
                    Err(may_error.as_ref().unwrap().clone())
                }
                None => {
                    let append_entries = ctx.consume_precommit_blocks();
                    // We must put the commit operation to global runtime, which will avoid the "dispatch dropped without returning error" in tower
                    GlobalIORuntime::instance().block_on(async move {
                        tracing::info!(
                            "insert: try to commit append entries:{}, elapsed:{}",
                            append_entries.len(),
                            start.elapsed().as_secs()
                        );

                        let copied_files = None;
                        table
                            .commit_insertion(ctx.clone(), append_entries, copied_files, overwrite)
                            .await?;

                        if stage_info.copy_options.purge {
                            info!(
                                "insert: try to purge files:{}, elapsed:{}",
                                all_source_files.len(),
                                start.elapsed().as_secs()
                            );
                            Self::try_purge_files(ctx.clone(), &stage_info, &all_source_files)
                                .await;
                        }

                        Ok(())
                    })
                }
            }
        });

        Ok(())
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
            InsertInputSource::Stage(opts) => {
                tracing::info!("insert: from stage with options {:?}", opts);
                self.build_insert_from_stage_pipeline(
                    table.clone(),
                    opts.clone(),
                    &mut build_res.main_pipeline,
                )
                .await?;
                return Ok(build_res);
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

                let values = exprs_to_scalar(
                    exprs,
                    &self.schema,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    bind_context,
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

async fn fill_default_value(
    binder: &mut ScalarBinder<'_>,
    map_exprs: &mut Vec<Expr>,
    field: &DataField,
    schema: &DataSchema,
) -> Result<()> {
    if let Some(default_expr) = field.default_expr() {
        let tokens = tokenize_sql(default_expr)?;
        let ast = parse_expr(&tokens, Dialect::PostgreSQL)?;
        let (mut scalar, _) = binder.bind(&ast).await?;
        scalar = wrap_cast(&scalar, field.data_type());

        let expr = scalar
            .as_expr()?
            .project_column_ref(|col| schema.index_of(&col.index.to_string()).unwrap());
        map_exprs.push(expr);
    } else {
        // If field data type is nullable, then we'll fill it with null.
        if field.data_type().is_nullable() {
            let expr = Expr::Constant {
                span: None,
                scalar: Scalar::Null,
                data_type: field.data_type().clone(),
            };
            map_exprs.push(expr);
        } else {
            let data_type = field.data_type().clone();
            let default_value = Scalar::default_value(&data_type);
            let expr = Expr::Constant {
                span: None,
                scalar: default_value,
                data_type,
            };
            map_exprs.push(expr);
        }
    }
    Ok(())
}

async fn exprs_to_scalar(
    exprs: Vec<AExpr>,
    schema: &DataSchemaRef,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &NameResolutionContext,
    bind_context: &mut BindContext,
    metadata: MetadataRef,
) -> Result<Vec<Scalar>> {
    let schema_fields_len = schema.fields().len();
    if exprs.len() != schema_fields_len {
        return Err(ErrorCode::TableSchemaMismatch(format!(
            "Table columns count is not match, expect {schema_fields_len}, input: {}, expr: {:?}",
            exprs.len(),
            exprs
        )));
    }
    let mut scalar_binder = ScalarBinder::new(
        bind_context,
        ctx.clone(),
        name_resolution_ctx,
        metadata.clone(),
        &[],
    );

    let mut map_exprs = Vec::with_capacity(exprs.len());
    for (i, expr) in exprs.iter().enumerate() {
        // `DEFAULT` in insert values will be parsed as `Expr::ColumnRef`.
        if let AExpr::ColumnRef { column, .. } = expr {
            if column.name.eq_ignore_ascii_case("default") {
                let field = schema.field(i);
                fill_default_value(&mut scalar_binder, &mut map_exprs, field, schema).await?;
                continue;
            }
        }

        let (mut scalar, data_type) = scalar_binder.bind(expr).await?;
        let field_data_type = schema.field(i).data_type();
        scalar = if field_data_type.remove_nullable() == DataType::Variant {
            match data_type.remove_nullable() {
                DataType::Boolean
                | DataType::Number(_)
                | DataType::Decimal(_)
                | DataType::Timestamp
                | DataType::Date
                | DataType::Bitmap
                | DataType::Variant => wrap_cast(&scalar, field_data_type),
                DataType::String => {
                    // parse string to JSON value
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "parse_json".to_string(),
                        params: vec![],
                        arguments: vec![scalar],
                    })
                }
                _ => {
                    if data_type == DataType::Null && field_data_type.is_nullable() {
                        scalar
                    } else {
                        return Err(ErrorCode::BadBytes(format!(
                            "unable to cast type `{}` to type `{}`",
                            data_type, field_data_type
                        )));
                    }
                }
            }
        } else {
            wrap_cast(&scalar, field_data_type)
        };
        let expr = scalar
            .as_expr()?
            .project_column_ref(|col| schema.index_of(&col.index.to_string()).unwrap());
        map_exprs.push(expr);
    }

    let mut operators = Vec::with_capacity(schema_fields_len);
    operators.push(BlockOperator::Map { exprs: map_exprs });

    let one_row_chunk = DataBlock::new(
        vec![BlockEntry {
            data_type: DataType::Number(NumberDataType::UInt8),
            value: Value::Scalar(Scalar::Number(NumberScalar::UInt8(1))),
        }],
        1,
    );
    let func_ctx = ctx.get_function_context()?;
    let mut expression_transform = CompoundBlockOperator {
        operators,
        ctx: func_ctx,
    };
    let res = expression_transform.transform(one_row_chunk)?;
    let scalars: Vec<Scalar> = res
        .columns()
        .iter()
        .skip(1)
        .map(|col| unsafe { col.value.as_ref().index_unchecked(0).to_owned() })
        .collect();
    Ok(scalars)
}

// TODO:(everpcpc) tmp copy from src/query/sql/src/planner/binder/copy.rs
// move to user stage module
async fn parse_stage_location(
    ctx: &Arc<QueryContext>,
    location: &str,
) -> Result<(StageInfo, String)> {
    let s: Vec<&str> = location.split('@').collect();
    // @my_ext_stage/abc/
    let names: Vec<&str> = s[1].splitn(2, '/').filter(|v| !v.is_empty()).collect();

    let stage = if names[0] == "~" {
        StageInfo::new_user_stage(&ctx.get_current_user()?.name)
    } else {
        UserApiProvider::instance()
            .get_stage(&ctx.get_tenant(), names[0])
            .await?
    };

    let path = names.get(1).unwrap_or(&"").trim_start_matches('/');

    Ok((stage, path.to_string()))
}
