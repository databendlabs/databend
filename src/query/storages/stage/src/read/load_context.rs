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

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_storage::FileParseError;

use crate::read::error_handler::ErrorHandler;

pub struct LoadContext {
    pub table_context: Arc<dyn TableContext>,
    pub func_ctx: FunctionContext,

    pub schema: TableSchemaRef,
    pub default_values: Option<Vec<RemoteExpr>>,
    pub pos_projection: Option<Vec<usize>>,
    pub is_copy: bool,

    pub file_format_options_ext: FileFormatOptionsExt,
    pub block_compact_thresholds: BlockThresholds,

    pub error_handler: ErrorHandler,
}

impl LoadContext {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        stage_table_info: &StageTableInfo,
        pos_projection: Option<Vec<usize>>,
        block_compact_thresholds: BlockThresholds,
    ) -> Result<Self> {
        let copy_options = &stage_table_info.stage_info.copy_options;
        let settings = ctx.get_settings();
        let func_ctx = ctx.get_function_context()?;
        let is_select = stage_table_info.is_select;
        let mut file_format_options_ext =
            FileFormatOptionsExt::create_from_settings(&settings, is_select)?;
        file_format_options_ext.disable_variant_check = copy_options.disable_variant_check;
        let on_error_mode = copy_options.on_error.clone();
        let fields = stage_table_info
            .schema
            .fields()
            .iter()
            .filter(|f| f.computed_expr().is_none())
            .cloned()
            .collect::<Vec<_>>();
        let schema = TableSchemaRefExt::create(fields);
        let default_values = stage_table_info.default_values.clone();
        let is_copy = ctx.get_query_kind() == QueryKind::CopyIntoTable;
        Ok(Self {
            table_context: ctx,
            func_ctx,
            block_compact_thresholds,
            schema,
            default_values,
            pos_projection,
            is_copy,
            file_format_options_ext,
            error_handler: ErrorHandler {
                on_error_mode,
                on_error_count: AtomicU64::new(0),
            },
        })
    }

    pub fn push_default_value(
        &self,
        column_builder: &mut ColumnBuilder,
        column_index: usize,
        required: bool,
    ) -> std::result::Result<(), FileParseError> {
        match &self.default_values {
            None => {
                if required {
                } else {
                    column_builder.push_default()
                }
            }
            Some(values) => {
                if let Some(remote_expr) = &values.get(column_index) {
                    let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
                    if let Expr::Constant { scalar, .. } = expr {
                        column_builder.push(scalar.as_ref());
                    } else {
                        let input = DataBlock::new(vec![], 1);
                        let evaluator = Evaluator::new(&input, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        let value =
                            evaluator
                                .run(&expr)
                                .map_err(|e| FileParseError::Unexpected {
                                    message: format!(
                                        "get error when eval default value: {}",
                                        e.message()
                                    ),
                                })?;
                        match value {
                            Value::Scalar(s) => {
                                column_builder.push(s.as_ref());
                            }
                            Value::Column(c) => {
                                let v = unsafe { c.index_unchecked(0) };
                                column_builder.push(v);
                            }
                        };
                    }
                }
            }
        }
        Ok(())
    }
}
