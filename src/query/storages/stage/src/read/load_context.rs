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
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_sql::binder::FieldDefaultExprEvaluator;
use databend_common_storage::FileParseError;

use crate::read::error_handler::ErrorHandler;

pub struct LoadContext {
    pub table_context: Arc<dyn TableContext>,

    pub schema: TableSchemaRef,
    pub field_default_expr_evaluator: Option<FieldDefaultExprEvaluator>,
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
        let field_default_expr_evaluator = match default_values {
            None => None,
            Some(values) => Some(FieldDefaultExprEvaluator::try_create(&ctx, values)?),
        };
        Ok(Self {
            table_context: ctx,
            block_compact_thresholds,
            schema,
            field_default_expr_evaluator,
            pos_projection,
            is_copy,
            file_format_options_ext,
            error_handler: ErrorHandler {
                on_error_mode,
                on_error_count: AtomicU64::new(0),
                on_error_map: None,
            },
        })
    }

    pub fn push_default_value(
        &self,
        column_builder: &mut ColumnBuilder,
        column_index: usize,
        required: bool,
    ) -> std::result::Result<(), FileParseError> {
        match &self.field_default_expr_evaluator {
            None => {
                if required {
                } else {
                    column_builder.push_default()
                }
            }
            Some(values) => column_builder.push(
                values
                    .get_scalar(column_index)
                    .map_err(|e| FileParseError::Unexpected {
                        message: format!("get error when eval default value: {}", e.message()),
                    })?
                    .as_ref(),
            ),
        }
        Ok(())
    }
}
