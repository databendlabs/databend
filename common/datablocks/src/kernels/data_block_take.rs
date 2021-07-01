// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn block_take_by_indices(raw: &DataBlock, indices: &[u32]) -> Result<DataBlock> {
        let columns = raw
            .columns()
            .iter()
            .map(|column| match column {
                DataColumn::Array(array) => {
                    let mut indices = indices.iter().map(|f| *f as usize);
                    let series = unsafe { array.take_iter_unchecked(&mut indices) }?;
                    Ok(DataColumn::Array(series))
                }
                DataColumn::Constant(v, _) => Ok(DataColumn::Constant(v.clone(), indices.len())),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(raw.schema().clone(), columns))
    }
}
