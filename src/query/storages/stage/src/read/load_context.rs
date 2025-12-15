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
use std::sync::atomic::AtomicU64;

use databend_common_ast::ast::OnErrorMode;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_storage::FileParseError;

use crate::read::default_expr_evaluator::DefaultExprEvaluator;
use crate::read::error_handler::ErrorHandler;

pub struct LoadContext {
    pub table_context: Arc<dyn TableContext>,
    pub internal_columns: Vec<InternalColumn>,

    pub schema: TableSchemaRef,
    pub default_exprs: Option<Vec<RemoteDefaultExpr>>,
    pub default_expr_evaluator: Option<Arc<DefaultExprEvaluator>>,
    pub pos_projection: Option<Vec<usize>>,
    pub is_copy: bool,
    pub stage_root: String,

    pub file_format_options_ext: FileFormatOptionsExt,
    pub block_compact_thresholds: BlockThresholds,

    pub error_handler: Arc<ErrorHandler>,
}

impl LoadContext {
    pub fn try_create_for_copy(
        ctx: Arc<dyn TableContext>,
        stage_table_info: &StageTableInfo,
        pos_projection: Option<Vec<usize>>,
        block_compact_thresholds: BlockThresholds,
        internal_columns: Vec<InternalColumn>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let is_select = stage_table_info.is_select;
        let mut file_format_options_ext =
            FileFormatOptionsExt::create_from_settings(&settings, is_select)?;
        file_format_options_ext.disable_variant_check = stage_table_info
            .copy_into_table_options
            .disable_variant_check;
        Self::try_create(
            ctx,
            stage_table_info.schema.clone(),
            file_format_options_ext,
            stage_table_info.default_exprs.clone(),
            pos_projection,
            block_compact_thresholds,
            internal_columns,
            stage_table_info.stage_root.clone(),
            stage_table_info.copy_into_table_options.on_error.clone(),
        )
    }
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        file_format_options_ext: FileFormatOptionsExt,
        default_exprs: Option<Vec<RemoteDefaultExpr>>,
        pos_projection: Option<Vec<usize>>,
        block_compact_thresholds: BlockThresholds,
        internal_columns: Vec<InternalColumn>,
        stage_root: String,
        on_error_mode: OnErrorMode,
    ) -> Result<Self> {
        let fields = schema
            .fields()
            .iter()
            .filter(|f| f.computed_expr().is_none())
            .cloned()
            .collect::<Vec<_>>();
        let schema = TableSchemaRefExt::create(fields);
        let default_expr_evaluator = if let Some(default_exprs) = &default_exprs {
            let func_ctx = ctx.get_function_context()?;
            Some(Arc::new(DefaultExprEvaluator::new(
                default_exprs.clone(),
                func_ctx,
                schema.clone(),
            )))
        } else {
            None
        };
        let is_copy = ctx.get_query_kind() == QueryKind::CopyIntoTable;
        Ok(Self {
            table_context: ctx,
            internal_columns,
            block_compact_thresholds,
            schema,
            default_expr_evaluator,
            default_exprs,
            pos_projection,
            is_copy,
            stage_root,
            file_format_options_ext,
            error_handler: Arc::new(ErrorHandler {
                on_error_mode,
                on_error_count: AtomicU64::new(0),
            }),
        })
    }

    pub fn push_default_value(
        &self,
        column_builder: &mut ColumnBuilder,
        column_index: usize,
        required: bool,
    ) -> std::result::Result<(), FileParseError> {
        match &self.default_expr_evaluator {
            None => {
                // not copy or not FieldDefault
                if !required {
                    column_builder.push_default()
                }
            }
            Some(values) => {
                values.push_default_value(column_builder, column_index)?;
            }
        }
        Ok(())
    }
}
