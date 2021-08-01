// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::iter::once;

use common_arrow::arrow::array::growable::make_growable;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::merge_sort::*;
use common_arrow::arrow::compute::sort as arrow_sort;
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
            .map(|f| Ok(block.try_array_by_name(&f.column_name)?.get_array_ref()))
            .collect::<Result<Vec<_>>>()?;

        let order_arrays = sort_columns_descriptions
            .iter()
            .zip(order_columns.iter())
            .map(|(f, array)| {
                Ok(arrow_sort::SortColumn {
                    values: array.as_ref(),
                    options: Some(arrow_sort::SortOptions {
                        descending: !f.asc,
                        nulls_first: f.nulls_first,
                    }),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let indices = arrow_sort::lexsort_to_indices(&order_arrays, limit)?;
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

        let sort_arrays = sort_columns_descriptions
            .iter()
            .map(|f| {
                let left = lhs.try_column_by_name(&f.column_name)?.clone();
                let left = left.to_array()?;

                let right = lhs.try_column_by_name(&f.column_name)?.clone();
                let right = right.to_array()?;

                Ok(vec![left.get_array_ref(), right.get_array_ref()])
            })
            .collect::<Result<Vec<_>>>()?;

        let sort_dyn_arrays = sort_arrays
            .iter()
            .map(|f| vec![f[0].as_ref(), f[1].as_ref()])
            .collect::<Vec<_>>();

        let sort_options = sort_columns_descriptions
            .iter()
            .map(|f| arrow_sort::SortOptions {
                descending: !f.asc,
                nulls_first: f.nulls_first,
            })
            .collect::<Vec<_>>();

        let sort_options_with_array = sort_dyn_arrays
            .iter()
            .zip(sort_options.iter())
            .map(|(s, opt)| {
                let paris: (&[&dyn Array], &SortOptions) = (s, opt);
                paris
            })
            .collect::<Vec<_>>();

        let comparator = build_comparator(&sort_options_with_array)?;
        let lhs_indices = (0, 0, lhs.num_rows());
        let rhs_indices = (1, 0, rhs.num_rows());
        let slices = merge_sort_slices(once(&lhs_indices), once(&rhs_indices), &comparator);
        let slices = Self::materialize_merge_indices(slices, limit);
        let fields = lhs.schema().fields();
        let columns = fields
            .iter()
            .map(|f| {
                let left = lhs.try_column_by_name(f.name())?;
                let right = rhs.try_column_by_name(f.name())?;

                let left = left.to_array()?;
                let right = right.to_array()?;

                let taked = Self::take_arrays_by_slices(
                    &[
                        left.get_array_ref().as_ref(),
                        right.get_array_ref().as_ref(),
                    ],
                    &slices,
                    limit,
                );
                let taked: ArrayRef = Arc::from(taked);

                Ok(DataColumn::Array(taked.into_series()))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(lhs.schema().clone(), columns))
    }

    fn materialize_merge_indices<
        'a,
        L: Iterator<Item = &'a MergeSlice>,
        R: Iterator<Item = &'a MergeSlice>,
    >(
        slices: MergeSortSlices<'a, L, R>,
        limit: Option<usize>,
    ) -> Vec<MergeSlice> {
        match limit {
            Some(limit) => {
                let mut v = Vec::with_capacity(limit);
                let mut current_len = 0;
                for (index, start, len) in slices {
                    v.push((index, start, len));

                    if len + current_len >= limit {
                        break;
                    } else {
                        current_len += len;
                    }
                }

                v
            }
            None => slices.into_iter().collect(),
        }
    }

    pub fn take_arrays_by_slices(
        arrays: &[&dyn Array],
        slices: &[MergeSlice],
        limit: Option<usize>,
    ) -> Box<dyn Array> {
        let slices = slices.into_iter();
        let len = arrays.iter().map(|array| array.len()).sum();

        let limit = limit.unwrap_or(len);
        let limit = limit.min(len);
        let mut growable = make_growable(arrays, false, limit);

        if limit != len {
            let mut current_len = 0;
            for (index, start, len) in slices {
                if len + current_len >= limit {
                    growable.extend(*index, *start, limit - current_len);
                    break;
                } else {
                    growable.extend(*index, *start, *len);
                    current_len += len;
                }
            }
        } else {
            for (index, start, len) in slices {
                growable.extend(*index, *start, *len);
            }
        }

        growable.as_box()
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
