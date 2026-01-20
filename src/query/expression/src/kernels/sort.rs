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

use databend_common_exception::Result;

use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Scalar;
use crate::SortCompare;
use crate::Value;
use crate::types::DataType;
use crate::visitor::ValueVisitor;

#[derive(Clone, Debug)]
pub struct SortColumnDescription {
    pub offset: usize,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Copy, Clone, Debug)]
pub enum LimitType {
    None,
    LimitRows(usize),
    LimitRank(usize),
}

impl LimitType {
    pub fn from_limit_rows(limit: Option<usize>) -> Self {
        match limit {
            Some(limit) => LimitType::LimitRows(limit),
            None => LimitType::None,
        }
    }
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
            let array = block.get_by_offset(desc.offset).value();
            sort_compare.visit_value(array)?;
            sort_compare.increment_column_index();
        }

        let permutations = sort_compare.take_permutation();
        DataBlock::take(block, permutations.as_slice())
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
    compare_columns(order_columns, length)
}

pub fn compare_columns(columns: Vec<Column>, length: usize) -> Result<Vec<u32>> {
    let descriptions = columns
        .iter()
        .enumerate()
        .map(|(idx, _)| SortColumnDescription {
            offset: idx,
            asc: true,
            nulls_first: false,
        })
        .collect::<Vec<_>>();

    let mut sort_compare = SortCompare::new(descriptions, length, LimitType::None);

    for array in columns {
        sort_compare.visit_value(Value::Column(array))?;
        sort_compare.increment_column_index();
    }

    Ok(sort_compare.take_permutation())
}
