// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::compute;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

pub struct SortColumnDescription {
    pub column_name: String,
    pub asc: bool,
    pub nulls_first: bool,
}

impl DataBlock {
    pub fn sort_block(
        block: &DataBlock,
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let order_columns = sort_columns_descriptions
            .iter()
            .map(|f| {
                Ok(compute::SortColumn {
                    values: block.try_array_by_name(&f.column_name)?.get_array_ref(),
                    options: Some(compute::SortOptions {
                        descending: !f.asc,
                        nulls_first: f.nulls_first,
                    }),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let indices = compute::lexsort_to_indices(&order_columns, limit)?;
        DataBlock::block_take_by_indices(block, &[], indices.values())
    }

    pub fn merge_sort_block(
        lhs: &DataBlock,
        rhs: &DataBlock,
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        if lhs.num_rows() == 0 {
            return Ok(rhs.clone());
        }

        if rhs.num_rows() == 0 {
            return Ok(lhs.clone());
        }

        let mut sort_columns = vec![];
        for block in [lhs, rhs].iter() {
            let columns = sort_columns_descriptions
                .iter()
                .map(|f| Ok(block.try_column_by_name(&f.column_name)?.clone()))
                .collect::<Result<Vec<_>>>()?;
            sort_columns.push(columns);
        }

        let sort_options = sort_columns_descriptions
            .iter()
            .map(|f| {
                Ok(compute::SortOptions {
                    descending: !f.asc,
                    nulls_first: f.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let indices = DataColumnCommon::merge_indices(
            &sort_columns[0],
            &sort_columns[1],
            &sort_options,
            limit,
        )?;

        let indices = match limit {
            Some(limit) => &indices[0..limit.min(indices.len())],
            _ => &indices,
        };

        let arrays = lhs
            .columns()
            .iter()
            .zip(rhs.columns().iter())
            .map(|(a, b)| {
                let _ = &indices;
                DataColumnCommon::merge_columns(a, b, indices)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(lhs.schema().clone(), arrays))
    }

    pub fn merge_sort_blocks(
        blocks: &[DataBlock],
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        match blocks.len() {
            0 => Result::Err(ErrorCode::EmptyData("Can't merge empty blocks")),
            1 => Ok(blocks[0].clone()),
            2 => DataBlock::merge_sort_block(
                &blocks[0],
                &blocks[1],
                sort_columns_descriptions,
                limit,
            ),
            _ => {
                let left = DataBlock::merge_sort_blocks(
                    &blocks[0..blocks.len() / 2],
                    sort_columns_descriptions,
                    limit,
                )?;
                let right = DataBlock::merge_sort_blocks(
                    &blocks[blocks.len() / 2..blocks.len()],
                    sort_columns_descriptions,
                    limit,
                )?;
                DataBlock::merge_sort_block(&left, &right, sort_columns_descriptions, limit)
            }
        }
    }
}
