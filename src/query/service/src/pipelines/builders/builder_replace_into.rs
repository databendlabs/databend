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

use databend_common_ast::ast::Expr;
use databend_common_ast::parser::parse_values;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_formats::FastFieldDecoderValues;
use databend_common_formats::FastValuesDecodeFallback;
use databend_common_formats::FastValuesDecoder;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_sql::BindContext;
use databend_common_sql::Metadata;
use databend_common_sql::MetadataRef;
use databend_common_sql::NameResolutionContext;
use parking_lot::RwLock;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // check if cast needed
    pub fn check_schema_cast(
        select_schema: Arc<DataSchema>,
        output_schema: Arc<DataSchema>,
    ) -> Result<bool> {
        let cast_needed = select_schema != output_schema;
        Ok(cast_needed)
    }
}

pub struct ValueSource {
    rows: Arc<Vec<Vec<Scalar>>>,
    schema: DataSchemaRef,
    is_finished: bool,
}

impl ValueSource {
    pub fn new(rows: Vec<Vec<Scalar>>, schema: DataSchemaRef) -> Self {
        Self {
            rows: Arc::new(rows),
            schema,
            is_finished: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for ValueSource {
    const NAME: &'static str = "ValueSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::with_capacity(f.data_type(), self.rows.len()))
            .collect::<Vec<_>>();

        for row in self.rows.as_ref() {
            for (field, column) in row.iter().zip(columns.iter_mut()) {
                column.push(field.as_ref());
            }
        }

        let columns = columns
            .into_iter()
            .map(|col| col.build())
            .collect::<Vec<_>>();
        let block = DataBlock::new_from_columns(columns);
        self.is_finished = true;
        Ok(Some(block))
    }
}

pub struct RawValueSource {
    data: String,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: NameResolutionContext,
    bind_context: BindContext,
    schema: DataSchemaRef,
    metadata: MetadataRef,
    start: usize,
    is_finished: bool,
}

impl RawValueSource {
    pub fn new(
        data: String,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: NameResolutionContext,
        schema: DataSchemaRef,
        start: usize,
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
            start,
            is_finished: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for RawValueSource {
    const NAME: &'static str = "RawValueSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        let format = self.ctx.get_format_settings()?;
        let rounding_mode = self
            .ctx
            .get_settings()
            .get_numeric_cast_option()
            .map(|s| s == "rounding")
            .unwrap_or(true);
        let field_decoder = FastFieldDecoderValues::create_for_insert(format, rounding_mode);

        let mut values_decoder = FastValuesDecoder::new(&self.data, &field_decoder);
        let estimated_rows = values_decoder.estimated_rows();

        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::with_capacity(f.data_type(), estimated_rows))
            .collect::<Vec<_>>();

        values_decoder.parse(&mut columns, self).await?;

        let columns = columns
            .into_iter()
            .map(|col| col.build())
            .collect::<Vec<_>>();
        let block = DataBlock::new_from_columns(columns);
        self.is_finished = true;
        Ok(Some(block))
    }
}

#[async_trait::async_trait]
impl FastValuesDecodeFallback for RawValueSource {
    async fn parse_fallback(&self, sql: &str) -> Result<Vec<Scalar>> {
        let res: Result<Vec<Scalar>> = try {
            let settings = self.ctx.get_settings();
            let sql_dialect = settings.get_sql_dialect()?;
            let tokens = tokenize_sql(sql).map_err(ErrorCode::from)?;
            let mut bind_context = self.bind_context.clone();
            let metadata = self.metadata.clone();

            let exprs = parse_values(&tokens, sql_dialect)
                .map_err(ErrorCode::from)?
                .into_iter()
                .map(|expr| match expr {
                    Expr::Placeholder { .. } => {
                        Err(ErrorCode::SyntaxException("unexpected placeholder"))
                    }
                    e => Ok(e),
                })
                .collect::<Result<Vec<_>>>()?;

            bind_context
                .exprs_to_scalar(
                    &exprs,
                    &self.schema,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    metadata,
                )
                .await?
        };
        res.map_err(|mut err: ErrorCode| {
            // The input for ValueSource is a sub-section of the original SQL. This causes
            // the error span to have an offset, so we adjust the span accordingly.
            if let Some(span) = err.span() {
                err = err.set_span(Some(
                    (span.start() + self.start..span.end() + self.start).into(),
                ));
            }
            err
        })
    }
}
