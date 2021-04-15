// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use anyhow::bail;
use anyhow::Result;
use common_arrow::arrow::compute;
use common_datavalues::merge_array;
use common_datavalues::merge_indices;

use crate::DataBlock;

pub struct SortColumnDescription {
    pub column_name: String,
    pub asc: bool,
    pub nulls_first: bool,
}

pub fn sort_block(
    block: &DataBlock,
    sort_columns_descriptions: &[SortColumnDescription],
    limit: Option<usize>,
) -> Result<DataBlock> {
    let order_columns = sort_columns_descriptions
        .iter()
        .map(|f| {
            Ok(compute::SortColumn {
                values: block.column_by_name(&f.column_name)?.clone(),
                options: Some(compute::SortOptions {
                    descending: !f.asc,
                    nulls_first: f.nulls_first,
                }),
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
    limit: Option<usize>,
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
                nulls_first: f.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let indices = merge_indices(&sort_arrays[0], &sort_arrays[1], &sort_options, limit)?;

    let indices = match limit {
        Some(limit) => &indices[0..limit.min(indices.len())],
        _ => &indices,
    };

    let arrays = lhs
        .columns()
        .iter()
        .zip(rhs.columns().iter())
        .map(|(a, b)| merge_array(a, b, &indices))
        .collect::<Result<Vec<_>>>()?;

    Ok(DataBlock::create(lhs.schema().clone(), arrays))
}

pub fn merge_sort_blocks(
    blocks: &[DataBlock],
    sort_columns_descriptions: &[SortColumnDescription],
    limit: Option<usize>,
) -> Result<DataBlock> {
    match blocks.len() {
        0 => bail!("Can't merge empty blocks"),
        1 => Ok(blocks[0].clone()),
        2 => merge_sort_block(&blocks[0], &blocks[1], sort_columns_descriptions, limit),
        _ => {
            let left = merge_sort_blocks(
                &blocks[0..blocks.len() / 2],
                sort_columns_descriptions,
                limit,
            )?;
            let right = merge_sort_blocks(
                &blocks[blocks.len() / 2..blocks.len()],
                sort_columns_descriptions,
                limit,
            )?;
            merge_sort_block(&left, &right, sort_columns_descriptions, limit)
        }
    }
}
