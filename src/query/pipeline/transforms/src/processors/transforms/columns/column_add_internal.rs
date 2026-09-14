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

use std::collections::BTreeMap;

use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableSchemaRef;

use crate::Transform;

pub struct TransformAddInternalColumns {
    internal_columns: BTreeMap<FieldIndex, InternalColumn>,
    schema: TableSchemaRef,
}

impl TransformAddInternalColumns
where Self: Transform
{
    pub fn new(
        internal_columns: BTreeMap<FieldIndex, InternalColumn>,
        schema: TableSchemaRef,
    ) -> Self {
        Self {
            internal_columns,
            schema,
        }
    }
}

impl Transform for TransformAddInternalColumns {
    const NAME: &'static str = "AddInternalColumnsTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = block.take_meta() {
            let internal_column_meta = InternalColumnMeta::downcast_from(meta)
                .ok_or_else(|| ErrorCode::Internal("It's a bug"))?;
            for internal_column in self.internal_columns.values() {
                let entry = internal_column.generate_column_values(
                    &internal_column_meta,
                    &block,
                    &self.schema,
                )?;
                block.add_entry(entry);
            }
            block = block.add_meta(internal_column_meta.inner)?;
        }
        Ok(block)
    }
}
