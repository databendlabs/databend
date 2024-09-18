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

use std::cmp::Ordering;
use std::sync::Arc;

use databend_common_arrow::arrow::array::ord as arrow_ord;
use databend_common_arrow::arrow::array::ord::DynComparator;
use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::PrimitiveArray;
use databend_common_arrow::arrow::compute::sort as arrow_sort;
use databend_common_arrow::arrow::datatypes::DataType as ArrowType;
use databend_common_arrow::arrow::error::Error as ArrowError;
use databend_common_arrow::arrow::error::Result as ArrowResult;
use databend_common_exception::Result;

use crate::converts::arrow2::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::converts::arrow2::ARROW_EXT_TYPE_EMPTY_MAP;
use crate::converts::arrow2::ARROW_EXT_TYPE_VARIANT;
use crate::types::DataType;
use crate::utils::arrow::column_to_arrow_array;
use crate::visitor::ValueVisitor;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Scalar;
use crate::SortCompare;
use crate::Value;

pub type AbortChecker = Arc<dyn CheckAbort + Send + Sync>;

pub trait CheckAbort {
    fn is_aborting(&self) -> bool;
    fn try_check_aborting(&self) -> Result<()>;
}

#[derive(Clone)]
pub struct SortColumnDescription {
    pub offset: usize,
    pub asc: bool,
    pub nulls_first: bool,
    pub is_nullable: bool,
}

#[derive(Copy, Clone, Debug)]
pub enum LimitType {
    None,
    LimitRows(usize),
    LimitRank(usize),
}

impl LimitType {
    pub fn limit_rows(&self, rows: usize) -> usize {
        match self {
            LimitType::LimitRows(limit) => *limit,
            _ => rows,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SortField {
    pub data_type: DataType,
    pub asc: bool,
    pub nulls_first: bool,
}

impl SortField {
    pub fn new(ty: DataType) -> Self {
        Self::new_with_options(ty, true, true)
    }

    pub fn new_with_options(ty: DataType, asc: bool, nulls_first: bool) -> Self {
        Self {
            data_type: ty,
            asc,
            nulls_first,
        }
    }
}

impl DataBlock {
    pub fn sort(
        block: &DataBlock,
        descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let limit = if let Some(l) = limit {
            LimitType::LimitRows(l)
        } else {
            LimitType::None
        };

        Self::sort_with_type(block, descriptions, limit)
    }

    pub fn sort_with_type(
        block: &DataBlock,
        descriptions: &[SortColumnDescription],
        limit: LimitType,
    ) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        if num_rows <= 1 || block.num_columns() == 0 {
            return Ok(block.clone());
        }
        let mut sort_compare = SortCompare::new(descriptions.to_owned(), num_rows, limit);

        for desc in descriptions.iter() {
            let array = block.get_by_offset(desc.offset).value.clone();
            sort_compare.visit_value(array)?;
            sort_compare.increment_column_index();
        }

        let permutations = sort_compare.take_permutation();
        DataBlock::take(block, &permutations, &mut None)
    }

    // TODO remove these
    #[allow(dead_code)]
    pub fn sort_old(
        block: &DataBlock,
        descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        if num_rows <= 1 {
            return Ok(block.clone());
        }
        let order_columns = descriptions
            .iter()
            .map(|d| column_to_arrow_array(block.get_by_offset(d.offset), num_rows))
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
        DataBlock::take(block, indices.values(), &mut None)
    }
}

fn compare_variant(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    let left = Column::from_arrow(left, &DataType::Variant)
        .unwrap()
        .as_variant()
        .cloned()
        .unwrap();
    let right = Column::from_arrow(right, &DataType::Variant)
        .unwrap()
        .as_variant()
        .cloned()
        .unwrap();
    Ok(Box::new(move |i, j| {
        let l = unsafe { left.index_unchecked(i) };
        let r = unsafe { right.index_unchecked(j) };
        jsonb::compare(l, r).unwrap()
    }))
}

fn compare_null() -> ArrowResult<DynComparator> {
    Ok(Box::new(move |_, _| Ordering::Equal))
}

fn compare_decimal256(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    let left = left
        .as_any()
        .downcast_ref::<databend_common_arrow::arrow::array::PrimitiveArray<
            databend_common_arrow::arrow::types::i256,
        >>()
        .unwrap()
        .clone();
    let right = right
        .as_any()
        .downcast_ref::<databend_common_arrow::arrow::array::PrimitiveArray<
            databend_common_arrow::arrow::types::i256,
        >>()
        .unwrap()
        .clone();

    Ok(Box::new(move |i, j| left.value(i).cmp(&right.value(j))))
}

fn build_compare(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    assert_eq!(left.data_type(), right.data_type());
    match left.data_type() {
        ArrowType::Extension(name, _, _) => match name.as_str() {
            ARROW_EXT_TYPE_VARIANT => compare_variant(left, right),
            ARROW_EXT_TYPE_EMPTY_ARRAY | ARROW_EXT_TYPE_EMPTY_MAP => compare_null(),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Sort not supported for data type {:?}",
                left.data_type()
            ))),
        },
        ArrowType::Null => compare_null(),
        ArrowType::Decimal256(_, _) => compare_decimal256(left, right),
        _ => arrow_ord::build_compare(left, right),
    }
}

pub fn compare_scalars(rows: Vec<Vec<Scalar>>, data_types: &[DataType]) -> Result<Vec<u32>> {
    let length = rows.len();
    let mut columns = data_types
        .iter()
        .map(|ty| ColumnBuilder::with_capacity(ty, length))
        .collect::<Vec<_>>();

    for row in rows.into_iter() {
        for (field, column) in row.into_iter().zip(columns.iter_mut()) {
            column.push(field.as_ref());
        }
    }

    let order_columns = columns
        .into_iter()
        .map(|builder| builder.build())
        .collect::<Vec<_>>();

    let descriptions = order_columns
        .iter()
        .enumerate()
        .map(|(idx, array)| SortColumnDescription {
            offset: idx,
            asc: true,
            nulls_first: false,
            is_nullable: array.data_type().is_nullable(),
        })
        .collect::<Vec<_>>();

    let mut sort_compare = SortCompare::new(descriptions, length, LimitType::None);

    for array in order_columns {
        sort_compare.visit_value(Value::Column(array))?;
        sort_compare.increment_column_index();
    }

    Ok(sort_compare.take_permutation())
}
