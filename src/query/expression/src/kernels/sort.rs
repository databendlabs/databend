// Copyright 2022 Datafuse Labs.
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

use std::cmp::Ordering;
use std::iter::once;
use std::sync::Arc;

use common_arrow::arrow::array::ord as arrow_ord;
use common_arrow::arrow::array::ord::DynComparator;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::compute::merge_sort as arrow_merge_sort;
use common_arrow::arrow::compute::merge_sort::build_comparator_impl;
use common_arrow::arrow::compute::sort as arrow_sort;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::error::Error as ArrowError;
use common_arrow::arrow::error::Result as ArrowResult;
use common_arrow::ArrayRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::types::AnyType;
use crate::types::DataType;
use crate::Chunk;
use crate::Column;
use crate::ColumnBuilder;
use crate::Value;
use crate::ARROW_EXT_TYPE_VARIANT;

pub type Aborting = Arc<Box<dyn Fn() -> bool + Send + Sync + 'static>>;

#[derive(Clone)]
pub struct SortColumnDescription {
    pub col_index: usize,
    pub asc: bool,
    pub nulls_first: bool,
}

fn column_to_arrow(column: &(Value<AnyType>, DataType), num_rows: usize) -> ArrayRef {
    match &column.0 {
        Value::Scalar(v) => {
            let builder = ColumnBuilder::repeat(&v.as_ref(), num_rows, &column.1);
            builder.build().as_arrow()
        }
        Value::Column(c) => c.as_arrow(),
    }
}

impl Chunk {
    pub fn sort(
        chunk: &Chunk,
        descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<Chunk> {
        let num_rows = chunk.num_rows();
        if num_rows <= 1 {
            return Ok(chunk.clone());
        }
        let order_columns = descriptions
            .iter()
            .map(|d| column_to_arrow(chunk.column(d.col_index), num_rows))
            .collect::<Vec<_>>();

        let order_arrays = descriptions
            .iter()
            .zip(order_columns.iter())
            .map(|(d, array)| arrow_sort::SortColumn {
                values: array.as_ref(),
                options: Some(arrow_sort::SortOptions {
                    descending: !d.asc,
                    nulls_first: d.nulls_first,
                }),
            })
            .collect::<Vec<_>>();

        let indices: PrimitiveArray<u32> =
            arrow_sort::lexsort_to_indices_impl(&order_arrays, limit, &build_compare)?;
        Chunk::take(chunk.clone(), indices.values())
    }

    // merge two chunks to one sorted chunk
    // require: lhs and rhs have been `convert_to_full`.
    fn two_way_merge_sort(
        chunks: &[Chunk],
        descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<Chunk> {
        assert!(chunks.len() == 2);

        let lhs = &chunks[0];
        let rhs = &chunks[1];
        let lhs_len = lhs.num_rows();
        let rhs_len = rhs.num_rows();
        if lhs_len == 0 {
            return Ok(rhs.clone());
        }
        if rhs_len == 0 {
            return Ok(lhs.clone());
        }

        let mut sort_options = Vec::with_capacity(descriptions.len());
        let sort_arrays = descriptions
            .iter()
            .map(|d| {
                let left = column_to_arrow(lhs.column(d.col_index), lhs_len);
                let right = column_to_arrow(rhs.column(d.col_index), rhs_len);
                sort_options.push(arrow_sort::SortOptions {
                    descending: !d.asc,
                    nulls_first: d.nulls_first,
                });
                vec![left, right]
            })
            .collect::<Vec<_>>();

        let sort_dyn_arrays = sort_arrays
            .iter()
            .map(|f| vec![f[0].as_ref(), f[1].as_ref()])
            .collect::<Vec<_>>();

        let sort_options_with_arrays = sort_dyn_arrays
            .iter()
            .zip(sort_options.iter())
            .map(|(arrays, opt)| (arrays as &[&dyn Array], opt))
            .collect::<Vec<_>>();

        let comparator = build_comparator_impl(&sort_options_with_arrays, &build_compare)?;
        let lhs_slice = (0, 0, lhs_len);
        let rhs_slice = (1, 0, rhs_len);

        let slices =
            arrow_merge_sort::merge_sort_slices(once(&lhs_slice), once(&rhs_slice), &comparator)
                .to_vec(limit);
        let chunk = Chunk::take_by_slices_limit_from_chunks(chunks, &slices, limit);
        Ok(chunk)
    }

    pub fn merge_sort(
        chunks: &[Chunk],
        descriptions: &[SortColumnDescription],
        limit: Option<usize>,
        aborting: Aborting,
    ) -> Result<Chunk> {
        match chunks.len() {
            0 => Result::Err(ErrorCode::EmptyData("Can't merge empty blocks")),
            1 => Ok(chunks[0].clone()),
            2 => {
                if aborting() {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }

                Chunk::two_way_merge_sort(chunks, descriptions, limit)
            }
            _ => {
                if aborting() {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }
                let left = Chunk::merge_sort(
                    &chunks[0..chunks.len() / 2],
                    descriptions,
                    limit,
                    aborting.clone(),
                )?;
                if aborting() {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }
                let right = Chunk::merge_sort(
                    &chunks[chunks.len() / 2..chunks.len()],
                    descriptions,
                    limit,
                    aborting.clone(),
                )?;
                if aborting() {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }
                Chunk::two_way_merge_sort(&[left, right], descriptions, limit)
            }
        }
    }
}

fn compare_variant(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    let left = Column::from_arrow(left).as_variant().unwrap().clone();
    let right = Column::from_arrow(right).as_variant().unwrap().clone();
    Ok(Box::new(move |i, j| {
        let (l, r) = unsafe { (left.index_unchecked(i), right.index_unchecked(j)) };
        common_jsonb::compare(l, r).unwrap()
    }))
}

fn compare_array(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    let left = Column::from_arrow(left).as_array().unwrap().clone();
    let right = Column::from_arrow(right).as_array().unwrap().clone();

    Ok(Box::new(move |i, j| {
        let (l, r) = unsafe { (left.index_unchecked(i), right.index_unchecked(j)) };
        l.partial_cmp(&r).unwrap_or(Ordering::Equal)
    }))
}

fn build_compare(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    match left.data_type() {
        ArrowType::LargeList(_) => compare_array(left, right),
        ArrowType::Extension(name, _, _) => {
            if name == ARROW_EXT_TYPE_VARIANT {
                compare_variant(left, right)
            } else {
                Err(ArrowError::NotYetImplemented(format!(
                    "Sort not supported for data type {:?}",
                    left.data_type()
                )))
            }
        }
        _ => arrow_ord::build_compare(left, right),
    }
}
