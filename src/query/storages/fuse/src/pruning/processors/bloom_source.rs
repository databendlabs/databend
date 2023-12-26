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

use std::collections::HashMap;
use std::sync::Arc;

use async_channel::Receiver;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_sources::AsyncSource;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use opendal::Operator;

use crate::io::BloomBlockFilterReader;

struct BloomSource {
    block_meta_receiver: Receiver<Arc<BlockMeta>>,
    func_ctx: FunctionContext,

    /// indices that should be loaded from filter block
    index_fields: Vec<TableField>,

    /// the expression that would be evaluate
    filter_expression: Expr<String>,

    /// pre calculated digest for constant Scalar
    scalar_map: HashMap<Scalar, u64>,

    /// the data accessor
    dal: Operator,

    /// the schema of data being indexed
    data_schema: TableSchemaRef,
}

#[async_trait::async_trait]
impl AsyncSource for BloomSource {
    const NAME: &'static str = "bloom source";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        loop {
            let block_meta = self.block_meta_receiver.recv().await;
            if block_meta.is_err() {
                return Ok(None);
            }
            let block_meta = block_meta.unwrap();
            let index_location = block_meta.bloom_filter_index_location.as_ref().unwrap();
            let index_size = block_meta.bloom_filter_index_size;
            let column_ids_of_indexed_block =
                block_meta.col_metas.keys().cloned().collect::<Vec<_>>();

            let version = index_location.1;
            // filter out columns that no longer exist in the indexed block
            let index_columns = self.index_fields.iter().try_fold(
                Vec::with_capacity(self.index_fields.len()),
                |mut acc, field| {
                    if column_ids_of_indexed_block.contains(&field.column_id()) {
                        acc.push(BloomIndex::build_filter_column_name(version, field)?);
                    }
                    Ok::<_, ErrorCode>(acc)
                },
            )?;
            let maybe_filter = index_location
                .read_block_filter(self.dal.clone(), &index_columns, index_size)
                .await;
        }
        todo!()
    }
}
