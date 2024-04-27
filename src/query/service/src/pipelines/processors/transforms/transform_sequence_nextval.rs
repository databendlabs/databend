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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

pub struct TransformSequenceNextval {
    ctx: Arc<QueryContext>,
    sequence: String,
    return_type: DataType,
}

impl TransformSequenceNextval {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        sequence: &str,
        return_type: &DataType,
    ) -> Result<Box<dyn Processor>> {
        Ok(AsyncTransformer::create(input, output, Self {
            ctx,
            sequence: sequence.to_owned(),
            return_type: return_type.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for TransformSequenceNextval {
    const NAME: &'static str = "SequenceSource";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        if data_block.is_empty() {
            return Ok(data_block);
        }
        let count = data_block.num_rows() as u64;
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_default_catalog()?;
        let req = GetSequenceNextValueReq {
            ident: SequenceIdent::new(&tenant, &self.sequence),
            count,
        };
        let resp = catalog.get_sequence_next_value(req).await?;
        let mut start = resp.start;
        let mut builder = ColumnBuilder::with_capacity(&self.return_type, data_block.num_rows());
        for _ in 0..count {
            let scalar = ScalarRef::Number(NumberScalar::UInt64(start));
            builder.push(scalar);
            start += 1;
        }
        let entry = BlockEntry {
            data_type: self.return_type.clone(),
            value: Value::Column(builder.build()),
        };

        data_block.add_column(entry);
        Ok(data_block)
    }
}
