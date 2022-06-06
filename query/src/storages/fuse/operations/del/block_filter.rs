//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::Expression;

use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::operations::del::mutations_collector::Deletion;
use crate::storages::fuse::FuseTable;

//  the "tranform filter" of delete plan
pub async fn delete_from_block(
    table: &FuseTable,
    block_meta: &BlockMeta,
    ctx: &Arc<QueryContext>,
    filter_column_ids: Vec<usize>,
    filter_expr: &Expression,
) -> Result<Deletion> {
    let mut filtering_whole_block = false;

    // extract the columns that are going to be filtered on
    let col_ids = {
        if filter_column_ids.is_empty() {
            filtering_whole_block = true;
            // To be optimized (in `interpreter_delete`, if we adhere the style of interpreter_select)
            // In this case, the expr being evaluated is unrelated to the value of rows:
            // - if the `filter_expr` is of "constant" function:
            //   for the whole block, whether all of the rows should be kept or dropped
            // - but, the expr may NOT be deterministic, e.g.
            //   A nullary non-constant func, which returns true, false or NULL randomly
            all_the_columns_ids(table)
        } else {
            filter_column_ids
        }
    };
    // read the cols that we are going to filtering on
    let reader = table.create_block_reader(ctx, col_ids)?;
    let data_block = reader.read_with_block_meta(block_meta).await?;

    // inverse the expr
    let inverse_expr = Expression::UnaryExpression {
        op: "not".to_string(),
        expr: Box::new(filter_expr.clone()),
    };

    let schema = table.table_info.schema();
    let expr_field = inverse_expr.to_data_field(&schema)?;
    let expr_schema = DataSchemaRefExt::create(vec![expr_field]);

    let expr_exec = ExpressionExecutor::try_create(
        ctx.clone(),
        "filter expression executor (delete) ",
        schema.clone(),
        expr_schema,
        vec![inverse_expr],
        false,
    )?;

    // get the single col data block, which indicates the rows should be kept/removed
    let filter_result = expr_exec.execute(&data_block)?;

    // read the whole block
    let whole_block = if filtering_whole_block {
        data_block
    } else {
        let whole_table_proj = all_the_columns_ids(table);
        let whole_block_reader = table.create_block_reader(ctx, whole_table_proj)?;
        whole_block_reader.read_with_block_meta(block_meta).await?
    };

    // returns the data remains after deletion
    let data_block = DataBlock::filter_block(whole_block, filter_result.column(0))?;
    let res = if data_block.num_rows() == block_meta.row_count as usize {
        Deletion::NothingDeleted
    } else {
        Deletion::Remains(data_block)
    };
    Ok(res)
}

fn all_the_columns_ids(table: &FuseTable) -> Vec<usize> {
    (0..table.table_info.schema().fields().len())
        .into_iter()
        .collect::<Vec<usize>>()
}
