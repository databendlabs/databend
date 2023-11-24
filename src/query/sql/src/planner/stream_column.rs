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

use common_catalog::plan::StreamColumn;
use common_catalog::plan::StreamColumnMeta;
use common_catalog::plan::StreamColumnType;
use common_exception::Result;
use common_expression::TableSchema;
use common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;

use crate::evaluator::BlockOperator;
use crate::evaluator::CompoundBlockOperator;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::ColumnBindingBuilder;
use crate::ScalarExpr;
use crate::Visibility;

pub const CURRENT_BLOCK_ID_COL_NAME: &str = "_current_block_id";
pub const CURRENT_BLOCK_ROW_NUM_COL_NAME: &str = "_current_block_row_num";

pub enum TransformStreamKind {
    Append(StreamColumnMeta),
    Mutation(CompoundBlockOperator),
}

/// Generate stream columns operator '_origin_block_id' and
/// '_origin_block_row_num' for mutation.
pub fn gen_mutation_stream_operator(
    schema: Arc<TableSchema>,
    change_tracking: bool,
) -> Result<(Vec<StreamColumn>, Vec<BlockOperator>)> {
    if !change_tracking {
        return Ok((vec![], vec![]));
    }

    let input_schema = schema.remove_virtual_computed_fields();
    let fields_num = input_schema.fields().len();
    let mut exprs = Vec::with_capacity(2);

    let origin_block_id_col =
        StreamColumn::new(ORIGIN_BLOCK_ID_COL_NAME, StreamColumnType::OriginBlockId);
    let block_id_type = Box::new(origin_block_id_col.data_type());
    let origin_block_id_index = input_schema
        .index_of(origin_block_id_col.column_name())
        .unwrap();
    let origin_block_id_scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            origin_block_id_col.column_name().to_string(),
            origin_block_id_index,
            block_id_type.clone(),
            Visibility::Visible,
        )
        .build(),
    });
    let current_block_id_scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            CURRENT_BLOCK_ID_COL_NAME.to_string(),
            fields_num,
            block_id_type,
            Visibility::Visible,
        )
        .build(),
    });
    let block_id_predicate = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "is_not_null".to_string(),
        params: vec![],
        arguments: vec![origin_block_id_scalar.clone()],
    });
    // if(is_not_null(_origin_block_id), _origin_block_id, _current_block_id)
    let block_id_scalar = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "if".to_string(),
        params: vec![],
        arguments: vec![
            block_id_predicate,
            origin_block_id_scalar,
            current_block_id_scalar,
        ],
    });
    exprs.push(
        block_id_scalar
            .as_expr()?
            .project_column_ref(|col| col.index),
    );

    let origin_row_num_col = StreamColumn::new(
        ORIGIN_BLOCK_ROW_NUM_COL_NAME,
        StreamColumnType::OriginRowNum,
    );
    let row_num_type = Box::new(origin_row_num_col.data_type());
    let origin_row_num_index = input_schema
        .index_of(origin_row_num_col.column_name())
        .unwrap();
    let origin_row_num_scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            origin_row_num_col.column_name().to_string(),
            origin_row_num_index,
            row_num_type.clone(),
            Visibility::Visible,
        )
        .build(),
    });
    let current_row_num_scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            CURRENT_BLOCK_ROW_NUM_COL_NAME.to_string(),
            fields_num + 1,
            row_num_type,
            Visibility::Visible,
        )
        .build(),
    });
    let row_num_predicate = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "is_not_null".to_string(),
        params: vec![],
        arguments: vec![origin_row_num_scalar.clone()],
    });
    // if(is_not_null(_origin_block_row_num), _origin_block_row_num, _current_block_row_num)
    let row_num_scalar = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "if".to_string(),
        params: vec![],
        arguments: vec![
            row_num_predicate,
            origin_row_num_scalar,
            current_row_num_scalar,
        ],
    });
    exprs.push(
        row_num_scalar
            .as_expr()?
            .project_column_ref(|col| col.index),
    );

    let mut projections = Vec::with_capacity(fields_num);
    for i in 0..fields_num {
        if i == origin_block_id_index {
            projections.push(fields_num + 2);
        } else if i == origin_row_num_index {
            projections.push(fields_num + 3);
        } else {
            projections.push(i);
        }
    }

    let operators = vec![
        BlockOperator::Map {
            exprs,
            projections: None,
        },
        BlockOperator::Project {
            projection: projections,
        },
    ];
    let stream_columns = vec![origin_block_id_col, origin_row_num_col];
    Ok((stream_columns, operators))
}
