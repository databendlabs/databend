// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use common_arrow::arrow::array::UInt32Builder;
use common_arrow::arrow::compute;

use crate::DataBlock;

impl DataBlock {
    pub fn block_take_by_indices(raw: &DataBlock, indices: &[u32]) -> Result<DataBlock> {
        let mut batch_indices: UInt32Builder = UInt32Builder::new(0);
        batch_indices.append_slice(indices)?;
        let batch_indices = batch_indices.finish();

        let takes = raw
            .columns()
            .iter()
            .map(|array| compute::take(array.as_ref(), &batch_indices, None).unwrap())
            .collect::<Vec<_>>();

        Ok(DataBlock::create(raw.schema().clone(), takes))
    }
}
