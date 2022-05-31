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
use crate::storages::fuse::operations::del::collector::Deletion;
use crate::storages::fuse::FuseTable;

pub async fn delete_from_block(
    table: &FuseTable,
    block_meta: &BlockMeta,
    ctx: &Arc<QueryContext>,
    filter_column_ids: Vec<usize>,
    expr: &Expression,
) -> Result<Deletion> {
    // read the cols that we are going to filtering on
    let data_block = {
        let reader = table.create_block_reader(ctx, filter_column_ids)?;
        reader.read_with_block_meta(block_meta).await?
    };

    // inverse the expr
    let inversed_expr = Expression::UnaryExpression {
        op: "!".to_string(),
        expr: Box::new(expr.clone()),
    };

    let schema = table.table_info.schema();
    let expr_field = inversed_expr.to_data_field(&schema)?;
    let expr_schema = DataSchemaRefExt::create(vec![expr_field]);

    let expr_exec = ExpressionExecutor::try_create(
        ctx.clone(),
        "filter expression executor (delete) ",
        schema.clone(),
        expr_schema,
        vec![inversed_expr],
        false,
    )?;

    // get the single col data block, which indicates the rows should be kept/removed
    let filter_block = expr_exec.execute(&data_block)?;

    // read the whole block
    let whole_block = {
        let whole_table_proj = (0..table.table_info.schema().fields().len())
            .into_iter()
            .collect::<Vec<usize>>();
        let whole_block_reader = table.create_block_reader(ctx, whole_table_proj)?;
        whole_block_reader.read_with_block_meta(block_meta).await?
    };

    // returns the data remains after deletion
    let data_block = DataBlock::filter_block(&whole_block, filter_block.column(0))?;
    let res = if data_block.num_rows() == block_meta.row_count as usize {
        // TODO: this works , but not acceptable (the whole data block is cloned here)
        Deletion::NothingDeleted
    } else {
        Deletion::Remains(data_block)
    };
    Ok(res)
}
