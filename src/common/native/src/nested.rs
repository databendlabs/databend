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

use std::ops::Range;

use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;

use crate::error::Result;

/// Descriptor of nested information of a field
#[derive(Debug, Clone, PartialEq)]
pub enum Nested {
    /// A primitive column
    Primitive(usize, bool, Option<Bitmap>),
    /// a list
    LargeList(ListNested),
    /// A struct column
    Struct(usize, bool, Option<Bitmap>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListNested {
    pub is_nullable: bool,
    pub offsets: Buffer<u64>,
    pub validity: Option<Bitmap>,
}

impl ListNested {
    pub fn new(offsets: Buffer<u64>, validity: Option<Bitmap>, is_nullable: bool) -> Self {
        Self {
            is_nullable,
            offsets,
            validity,
        }
    }
}

pub type NestedState = Vec<Nested>;

impl Nested {
    pub fn length(&self) -> usize {
        match self {
            Nested::Primitive(len, _, _) => *len,
            Nested::LargeList(l) => l.offsets.len(),
            Nested::Struct(len, _, _) => *len,
        }
    }

    pub fn is_nullable(&self) -> bool {
        match self {
            Nested::Primitive(_, b, _) => *b,
            Nested::LargeList(l) => l.is_nullable,
            Nested::Struct(_, b, _) => *b,
        }
    }

    pub fn inner(&self) -> (Buffer<u64>, &Option<Bitmap>) {
        match self {
            Nested::Primitive(_, _, v) => (Buffer::new(), v),
            Nested::LargeList(l) => {
                let start = *l.offsets.first().unwrap();
                let buffer = if start == 0 {
                    l.offsets.clone()
                } else {
                    l.offsets.iter().map(|x| *x - start).collect()
                };
                (buffer, &l.validity)
            }
            Nested::Struct(_, _, v) => (Buffer::new(), v),
        }
    }

    pub fn validity(&self) -> &Option<Bitmap> {
        match self {
            Nested::Primitive(_, _, v) => v,
            Nested::LargeList(l) => &l.validity,
            Nested::Struct(_, _, v) => v,
        }
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Nested::LargeList(_))
    }
}

/// Constructs the necessary `Vec<Vec<Nested>>` to write the rep and def levels of `column` to parquet
pub fn to_nested(column: &Column) -> Result<Vec<Vec<Nested>>> {
    let mut nested = vec![];

    to_nested_recursive(column, &mut nested, vec![])?;
    Ok(nested)
}

pub fn is_nested_type(t: &TableDataType) -> bool {
    matches!(
        t,
        TableDataType::Tuple { .. } | TableDataType::Array(_) | TableDataType::Map(_)
    )
}

/// Slices the [`column`] to `Column` and `Vec<Nested>`.
pub fn slice_nest_column(
    primitive_column: &mut Column,
    nested: &mut [Nested],
    mut current_offset: usize,
    mut current_length: usize,
) {
    for nested in nested.iter_mut() {
        match nested {
            Nested::LargeList(l_nested) => {
                l_nested.offsets.slice(current_offset, current_length + 1);
                if let Some(validity) = l_nested.validity.as_mut() {
                    validity.slice(current_offset, current_length)
                };

                let r = *l_nested.offsets.last().unwrap() - *l_nested.offsets.first().unwrap();
                current_length = r as usize;
                current_offset = *l_nested.offsets.first().unwrap() as usize;
            }
            Nested::Struct(length, _, validity) => {
                *length = current_length;
                if let Some(validity) = validity.as_mut() {
                    validity.slice(current_offset, current_length)
                };
            }
            Nested::Primitive(length, _, validity) => {
                *length = current_length;
                if let Some(validity) = validity.as_mut() {
                    validity.slice(current_offset, current_length)
                };
                primitive_column.slice(Range {
                    start: current_offset,
                    end: current_offset + current_length,
                });
            }
        }
    }
}

fn to_nested_recursive(
    column: &Column,
    nested: &mut Vec<Vec<Nested>>,
    mut parents: Vec<Nested>,
) -> Result<()> {
    let nullable = column.as_nullable().is_some();
    let validity = column.validity().1.cloned();

    match column.remove_nullable() {
        Column::Tuple(values) => {
            parents.push(Nested::Struct(column.len(), nullable, validity));
            for column in values {
                to_nested_recursive(&column, nested, parents.clone())?;
            }
        }
        Column::Array(inner) => {
            parents.push(Nested::LargeList(ListNested {
                is_nullable: nullable,
                offsets: inner.offsets.clone(),
                validity,
            }));
            to_nested_recursive(&inner.values, nested, parents)?;
        }
        other => {
            parents.push(Nested::Primitive(column.len(), nullable, validity));
            nested.push(parents);
        }
    }

    Ok(())
}

/// Convert [`column`] to `Vec<Column>` leaves in DFS order.
pub fn to_leaves(column: &Column) -> Vec<Column> {
    let mut leaves = vec![];
    to_leaves_recursive(column, &mut leaves);
    leaves
}

fn to_leaves_recursive(column: &Column, leaves: &mut Vec<Column>) {
    match column {
        Column::Tuple(cs) => {
            cs.iter().for_each(|a| to_leaves_recursive(a, leaves));
        }
        Column::Array(col) => {
            to_leaves_recursive(&col.values, leaves);
        }
        Column::Map(col) => {
            to_leaves_recursive(&col.values, leaves);
        }
        // Handle nullable columns by recursing into their inner value
        Column::Nullable(inner) => to_leaves_recursive(&inner.column, leaves),
        // All primitive/leaf types
        _ => leaves.push(column.clone()),
    }
}

/// The initial info of nested data types.
/// The initial info of nested data types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitNested {
    /// Primitive data types
    Primitive(bool),
    /// List data types
    List(bool),
    /// Struct data types
    Struct(bool),
}

impl InitNested {
    pub fn is_nullable(&self) -> bool {
        match self {
            InitNested::Primitive(b) => *b,
            InitNested::List(b) => *b,
            InitNested::Struct(b) => *b,
        }
    }
}

pub fn create_list(data_type: TableDataType, nested: &mut NestedState, values: Column) -> Column {
    let n = nested.pop().unwrap();
    let (offsets, validity) = n.inner();
    let col = Column::Map(Box::new(ArrayColumn::<AnyType> { values, offsets }));

    if data_type.is_nullable() {
        col.wrap_nullable(validity.clone())
    } else {
        col
    }
}

/// Creates a new [`Mapcolumn`].
pub fn create_map(data_type: TableDataType, nested: &mut NestedState, values: Column) -> Column {
    let n = nested.pop().unwrap();
    let (offsets, validity) = n.inner();
    let col = Column::Map(Box::new(ArrayColumn::<AnyType> { values, offsets }));
    if data_type.is_nullable() {
        col.wrap_nullable(validity.clone())
    } else {
        col
    }
}

pub fn create_struct(
    is_nullable: bool,
    nested: &mut Vec<NestedState>,
    values: Vec<Column>,
) -> (NestedState, Column) {
    let mut nest = nested.pop().unwrap();
    let n = nest.pop().unwrap();
    let (_, validity) = n.inner();

    let col = Column::Tuple(values);
    if is_nullable {
        (nest, col.wrap_nullable(validity.clone()))
    } else {
        (nest, col)
    }
}
