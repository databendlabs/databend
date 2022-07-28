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

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_planners::find_column_exprs;
use common_planners::Expression;
use common_tracing::tracing;
use opendal::Operator;

use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::index::BloomFilterExprEvalResult;
use crate::storages::index::BloomFilterIndexer;

#[async_trait::async_trait]
pub trait BoolFilterPredicate {
    async fn eval(&self, block_bloom_filter_path: &str) -> common_exception::Result<bool>;
}

pub struct BlockBloomFilterPredicate<'a> {
    cols: Vec<String>,
    expr: &'a Expression,
    dal: &'a Operator,
    schema: &'a DataSchemaRef,
    ctx: &'a Arc<dyn TableContext>,
}

impl<'a> BlockBloomFilterPredicate<'a> {
    pub fn new(
        filter_block_column_names: Vec<String>,
        filter_expression: &'a Expression,
        dal: &'a Operator,
        schema: &'a DataSchemaRef,
        ctx: &'a Arc<dyn TableContext>,
    ) -> Self {
        Self {
            cols: filter_block_column_names,
            expr: filter_expression,
            dal,
            schema,
            ctx,
        }
    }
}

#[async_trait::async_trait]
impl BoolFilterPredicate for BlockBloomFilterPredicate<'_> {
    async fn eval(&self, loc: &str) -> common_exception::Result<bool> {
        filter_block_by_bloom_index(
            self.ctx,
            self.dal.clone(),
            self.schema,
            self.expr,
            &self.cols,
            loc,
        )
        .await
    }
}

pub(crate) struct NonPruner;

#[async_trait::async_trait]
impl BoolFilterPredicate for NonPruner {
    async fn eval(&self, _loc: &str) -> common_exception::Result<bool> {
        Ok(true)
    }
}

pub async fn filter_block_by_bloom_index(
    ctx: &Arc<dyn TableContext>,
    dal: Operator,
    schema: &DataSchemaRef,
    filter_expr: &Expression,
    bloom_index_col_names: &[String],
    block_path: &str,
) -> common_exception::Result<bool> {
    let bloom_idx_location = TableMetaLocationGenerator::block_bloom_index_location(block_path);
    let filter_block =
        load_bloom_filter_by_columns(dal, bloom_index_col_names, &bloom_idx_location).await?;
    let ctx = ctx.clone();
    let index = BloomFilterIndexer::from_bloom_block(schema.clone(), filter_block, ctx)?;
    Ok(BloomFilterExprEvalResult::False != index.eval(filter_expr)?)
}

#[tracing::instrument(level = "debug", skip(dal))]
async fn load_bloom_filter_by_columns(
    dal: Operator,
    projection: &[String],
    location: &str,
) -> common_exception::Result<DataBlock> {
    use common_arrow::arrow::io::parquet::read::read_columns_many_async;
    use common_arrow::arrow::io::parquet::read::read_metadata_async;
    use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
    use common_datavalues::ToDataType;
    use common_datavalues::Vu8;
    let object = dal.object(location);
    let mut reader = object.seekable_reader(0..);
    let file_meta = read_metadata_async(&mut reader).await?;
    let row_groups = file_meta.row_groups;

    // TODO filter out columns that not in the bloom block
    let fields = projection
        .iter()
        .map(|name| DataField::new(name, Vu8::to_data_type()))
        .collect::<Vec<_>>();
    let row_group = &row_groups[0];
    let arrow_fields = fields.iter().map(|f| f.to_arrow()).collect::<Vec<_>>();
    let arrays = read_columns_many_async(
        || Box::pin(async { Ok(object.seekable_reader(0..)) }),
        row_group,
        arrow_fields,
        None,
    )
    .await?;

    let schema = Arc::new(DataSchema::new(fields));
    if let Some(next_item) = RowGroupDeserializer::new(arrays, row_group.num_rows(), None).next() {
        let chunk = next_item?;
        DataBlock::from_chunk(&schema, &chunk)
    } else {
        Ok(DataBlock::empty_with_schema(schema))
    }
}

pub fn column_names_of_expression(filter_expr: &Expression) -> Vec<String> {
    find_column_exprs(&[filter_expr.clone()])
        .iter()
        .map(|e| BloomFilterIndexer::to_bloom_column_name(&e.column_name()))
        .collect::<Vec<_>>()
}

pub fn new<'a>(
    ctx: &'a Arc<dyn TableContext>,
    filter_expr: Option<&'a Expression>,
    schema: &'a DataSchemaRef,
    dal: &'a Operator,
) -> Box<dyn BoolFilterPredicate + Send + Sync + 'a> {
    if let Some(expr) = filter_expr {
        let filter_block_cols = column_names_of_expression(expr);
        Box::new(BlockBloomFilterPredicate::new(
            filter_block_cols,
            expr,
            &dal,
            &schema,
            ctx,
        ))
    } else {
        Box::new(NonPruner)
    }
}
