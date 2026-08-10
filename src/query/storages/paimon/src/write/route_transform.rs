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

use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::BinaryType;
use databend_common_pipeline_transforms::processors::Transform;
use paimon::spec::TableSchema;

use crate::error::map_paimon_error;
use crate::write::router::PaimonWriteRouter;
use crate::write::writer::data_block_to_paimon_batch;

/// Appends a non-null Binary `__paimon_route_key` column for GlobalHash Exchange.
pub struct TransformPaimonWriteRoute {
    router: PaimonWriteRouter,
    arrow_schema: Arc<ArrowSchema>,
}

impl TransformPaimonWriteRoute {
    pub fn try_create(schema: &TableSchema) -> Result<Self> {
        let arrow_schema =
            paimon::arrow::build_target_arrow_schema(schema.fields()).map_err(map_paimon_error)?;
        Ok(Self {
            router: PaimonWriteRouter::try_create(schema)?,
            arrow_schema,
        })
    }
}

impl Transform for TransformPaimonWriteRoute {
    const NAME: &'static str = "TransformPaimonWriteRoute";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        if block.is_empty() {
            block.add_column(BinaryType::from_data(Vec::<Vec<u8>>::new()));
            return Ok(block);
        }

        // Keep original columns for downstream projection; convert a copy for routing.
        let batch = data_block_to_paimon_batch(block.clone(), &self.arrow_schema)?;
        let routes = self.router.route_batch(&batch)?;
        let keys: Vec<Vec<u8>> = routes.into_iter().map(|r| r.key).collect();
        block.add_column(BinaryType::from_data(keys));
        Ok(block)
    }
}
