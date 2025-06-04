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

use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
use databend_common_pipeline_transforms::sort::RowConverter;
use databend_common_pipeline_transforms::sort::Rows;
use databend_common_pipeline_transforms::Transform;

pub struct TransformAddOrderColumn<R, C> {
    row_converter: C,
    sort_desc: Arc<[SortColumnDescription]>,
    _r: PhantomData<R>,
}

impl<R, C> TransformAddOrderColumn<R, C>
where
    R: Rows,
    C: RowConverter<R>,
{
    pub fn try_new(sort_desc: Arc<[SortColumnDescription]>, schema: DataSchemaRef) -> Result<Self> {
        let row_converter = C::create(&sort_desc, schema.clone())?;
        Ok(Self {
            row_converter,
            sort_desc,
            _r: PhantomData,
        })
    }
}

impl<R, C> Transform for TransformAddOrderColumn<R, C>
where
    R: Rows + 'static,
    C: RowConverter<R> + Send + 'static,
{
    const NAME: &'static str = "TransformAddOrderColumn";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let order_by_cols = self
            .sort_desc
            .iter()
            .map(|desc| data.get_by_offset(desc.offset).clone())
            .collect::<Vec<_>>();
        let rows = self
            .row_converter
            .convert(&order_by_cols, data.num_rows())?;
        let order_col = rows.to_column();
        data.add_column(BlockEntry {
            data_type: order_col.data_type(),
            value: Value::Column(order_col),
        });
        Ok(data)
    }
}
