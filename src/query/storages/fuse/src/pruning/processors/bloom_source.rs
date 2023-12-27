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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use async_channel::Receiver;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;

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

impl BloomSource {
    pub fn create(
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        dal: Operator,
        filter_expr: Option<&Expr<String>>,
        bloom_index_cols: BloomIndexColumns,
        block_meta_receiver: Receiver<Arc<BlockMeta>>,
    ) -> Result<Option<Self>> {
        if let Some(expr) = filter_expr {
            let bloom_columns_map =
                bloom_index_cols.bloom_index_fields(schema.clone(), BloomIndex::supported_type)?;
            let bloom_column_fields = bloom_columns_map.values().cloned().collect::<Vec<_>>();
            let point_query_cols = BloomIndex::find_eq_columns(expr, bloom_column_fields)?;

            if !point_query_cols.is_empty() {
                // convert to filter column names
                let mut filter_fields = Vec::with_capacity(point_query_cols.len());
                let mut scalar_map = HashMap::<Scalar, u64>::new();
                for (field, scalar, ty) in point_query_cols.into_iter() {
                    filter_fields.push(field);
                    if let Entry::Vacant(e) = scalar_map.entry(scalar.clone()) {
                        let digest = BloomIndex::calculate_scalar_digest(&func_ctx, &scalar, &ty)?;
                        e.insert(digest);
                    }
                }

                let bloom_source = BloomSource {
                    block_meta_receiver,
                    func_ctx,
                    index_fields: filter_fields,
                    filter_expression: expr.clone(),
                    scalar_map,
                    dal,
                    data_schema: schema.clone(),
                };
                return Ok(Some(bloom_source));
            }
        }
        Ok(None)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BloomFilterBytes {
    bytes: Vec<Vec<u8>>,
}

#[typetag::serde(name = "bloom_filter_bytes")]
impl BlockMetaInfo for BloomFilterBytes {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("equals is unimplemented for SegmentBytes")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[async_trait::async_trait]
impl AsyncSource for BloomSource {
    const NAME: &'static str = "bloom source";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let block_meta = self.block_meta_receiver.recv().await;
        if block_meta.is_err() {
            return Ok(None);
        }
        let block_meta = block_meta.unwrap();
        let index_location = block_meta.bloom_filter_index_location.as_ref().unwrap();
        let index_size = block_meta.bloom_filter_index_size;
        let column_ids_of_indexed_block = block_meta.col_metas.keys().cloned().collect::<Vec<_>>();

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
        let bloom_index = index_location
            .read_block_filter(self.dal.clone(), &index_columns, index_size)
            .await?;
        return Ok(Some(DataBlock::empty_with_meta(Box::new(
            BloomFilterBytes { bytes: bloom_index },
        ))));
    }
}
