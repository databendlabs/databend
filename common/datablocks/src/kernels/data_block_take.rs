// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn block_take_by_indices(
        raw: &DataBlock,
        constant_columns: &[String],
        indices: &[u32],
    ) -> Result<DataBlock> {
        if indices.is_empty() {
            return Ok(DataBlock::empty_with_schema(raw.schema().clone()));
        }
        let fields = raw.schema().fields();
        let columns = fields
            .iter()
            .map(|f| {
                let column = raw.try_column_by_name(f.name())?;
                if constant_columns.contains(f.name()) {
                    let v = column.try_get(indices[0] as usize)?;
                    Ok(DataColumn::Constant(v, indices.len()))
                } else {
                    match column {
                        DataColumn::Array(array) => {
                            let mut indices = indices.iter().map(|f| *f as usize);
                            let series = unsafe { array.take_iter_unchecked(&mut indices) }?;
                            Ok(DataColumn::Array(series))
                        }
                        DataColumn::Constant(v, _) => {
                            Ok(DataColumn::Constant(v.clone(), indices.len()))
                        }
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(raw.schema().clone(), columns))
    }
}
