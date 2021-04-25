// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::bail;
use anyhow::Result;
use common_arrow::arrow::array::UInt32Builder;
use common_arrow::arrow::compute;
use common_datavalues::DataArrayMerge;

use crate::DataBlock;

pub struct SortColumnDescription {
    pub column_name: String,
    pub asc: bool,
    pub nulls_first: bool
}

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

    pub fn concat_blocks(blocks: &[DataBlock]) -> Result<DataBlock> {
        if blocks.is_empty() {
            bail!("Can't concat empty blocks",);
        }

        let first_block = &blocks[0];
        for block in blocks.iter() {
            if block.schema().ne(first_block.schema()) {
                bail!("Schema not matched");
            }
        }

        let mut values = Vec::with_capacity(first_block.num_columns());
        for (i, _f) in blocks[0].schema().fields().iter().enumerate() {
            let mut arr = Vec::with_capacity(blocks.len());
            for block in blocks.iter() {
                arr.push(block.column(i).as_ref());
            }
            values.push(compute::concat(&arr)?);
        }

        Ok(DataBlock::create(first_block.schema().clone(), values))
    }

    pub fn sort_block(
        block: &DataBlock,
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>
    ) -> Result<DataBlock> {
        let order_columns = sort_columns_descriptions
            .iter()
            .map(|f| {
                Ok(compute::SortColumn {
                    values: block.column_by_name(&f.column_name)?.clone(),
                    options: Some(compute::SortOptions {
                        descending: !f.asc,
                        nulls_first: f.nulls_first
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let indices = compute::lexsort_to_indices(&order_columns, limit)?;
        let columns = block
            .columns()
            .iter()
            .map(|c| compute::take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(DataBlock::create(block.schema().clone(), columns))
    }

    pub fn merge_sort_block(
        lhs: &DataBlock,
        rhs: &DataBlock,
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>
    ) -> Result<DataBlock> {
        if lhs.num_rows() == 0 {
            return Ok(rhs.clone());
        }

        if rhs.num_rows() == 0 {
            return Ok(lhs.clone());
        }

        let mut sort_arrays = vec![];
        for block in [lhs, rhs].iter() {
            let columns = sort_columns_descriptions
                .iter()
                .map(|f| Ok(block.column_by_name(&f.column_name)?.clone()))
                .collect::<Result<Vec<_>>>()?;
            sort_arrays.push(columns);
        }

        let sort_options = sort_columns_descriptions
            .iter()
            .map(|f| {
                Ok(compute::SortOptions {
                    descending: !f.asc,
                    nulls_first: f.nulls_first
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let indices =
            DataArrayMerge::merge_indices(&sort_arrays[0], &sort_arrays[1], &sort_options, limit)?;

        let indices = match limit {
            Some(limit) => &indices[0..limit.min(indices.len())],
            _ => &indices
        };

        let arrays = lhs
            .columns()
            .iter()
            .zip(rhs.columns().iter())
            .map(|(a, b)| DataArrayMerge::merge_array(a, b, &indices))
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(lhs.schema().clone(), arrays))
    }

    pub fn merge_sort_blocks(
        blocks: &[DataBlock],
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>
    ) -> Result<DataBlock> {
        match blocks.len() {
            0 => bail!("Can't merge empty blocks"),
            1 => Ok(blocks[0].clone()),
            2 => DataBlock::merge_sort_block(
                &blocks[0],
                &blocks[1],
                sort_columns_descriptions,
                limit
            ),
            _ => {
                let left = DataBlock::merge_sort_blocks(
                    &blocks[0..blocks.len() / 2],
                    sort_columns_descriptions,
                    limit
                )?;
                let right = DataBlock::merge_sort_blocks(
                    &blocks[blocks.len() / 2..blocks.len()],
                    sort_columns_descriptions,
                    limit
                )?;
                DataBlock::merge_sort_block(&left, &right, sort_columns_descriptions, limit)
            }
        }
    }
}
