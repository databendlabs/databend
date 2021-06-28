// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::UInt32Builder;
use common_arrow::arrow::compute;
use common_datavalues::DataColumnarValue;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn block_take_by_indices(raw: &DataBlock, indices: &[u32]) -> Result<DataBlock> {
        let columns = raw
            .columns()
            .iter()
            .map(|column| match column {
                DataColumnarValue::Array(array) => {
                    let taked_array = compute::take(array.as_ref(), &batch_indices, None)?;
                    Ok(DataColumnarValue::Array(taked_array))
                }
                DataColumnarValue::Constant(v, _) => {
                    Ok(DataColumnarValue::Constant(v.clone(), indices.len()))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(raw.schema().clone(), columns))
    }
}
