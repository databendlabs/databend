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

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::FixedSizeListArray;
use arrow_array::LargeListArray;
use arrow_array::ListArray;
use arrow_array::MapArray;
use arrow_array::OffsetSizeTrait;
use arrow_array::StructArray;
use arrow_buffer::NullBuffer;
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use arrow_schema::Field;

use crate::error::Result;

/// Descriptor of nested information of a field
#[derive(Debug, Clone)]
pub enum Nested {
    /// A primitive array
    Primitive(usize, bool, Option<NullBuffer>),
    /// a list
    List(ListNested<i32>),
    /// a list
    LargeList(ListNested<i64>),
    /// A struct array
    Struct(usize, bool, Option<NullBuffer>),
}

#[derive(Debug, Clone)]
pub struct ListNested<O: OffsetSizeTrait> {
    pub is_nullable: bool,
    pub offsets: OffsetBuffer<O>,
    pub nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> ListNested<O> {
    pub fn new(offsets: OffsetBuffer<O>, nulls: Option<NullBuffer>, is_nullable: bool) -> Self {
        Self {
            is_nullable,
            offsets,
            nulls,
        }
    }
}

pub type NestedState = Vec<Nested>;

impl Nested {
    pub fn length(&self) -> usize {
        match self {
            Nested::Primitive(len, _, _) => *len,
            Nested::List(l) => l.offsets.len(),
            Nested::LargeList(l) => l.offsets.len(),
            Nested::Struct(len, _, _) => *len,
        }
    }

    pub fn is_nullable(&self) -> bool {
        match self {
            Nested::Primitive(_, b, _) => *b,
            Nested::List(l) => l.is_nullable,
            Nested::LargeList(l) => l.is_nullable,
            Nested::Struct(_, b, _) => *b,
        }
    }

    pub fn inner(&self) -> (OffsetBuffer<i64>, &Option<NullBuffer>) {
        match self {
            Nested::Primitive(_, _, v) => (OffsetBuffer::new_empty(), v),
            Nested::List(l) => {
                let start = *l.offsets.first().unwrap();
                let buffer =
                    OffsetBuffer::from_lengths(l.offsets.iter().map(|x| (*x - start) as usize));
                (buffer, &l.nulls)
            }
            Nested::LargeList(l) => {
                let start = *l.offsets.first().unwrap();
                let buffer = if start == 0 {
                    l.offsets.clone()
                } else {
                    OffsetBuffer::from_lengths(l.offsets.iter().map(|x| (*x - start) as usize))
                };
                (buffer, &l.nulls)
            }
            Nested::Struct(_, _, v) => (OffsetBuffer::new_empty(), v),
        }
    }

    pub fn nulls(&self) -> &Option<NullBuffer> {
        match self {
            Nested::Primitive(_, _, v) => v,
            Nested::List(l) => &l.nulls,
            Nested::LargeList(l) => &l.nulls,
            Nested::Struct(_, _, v) => v,
        }
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Nested::List(_) | Nested::LargeList(_))
    }
}

/// Constructs the necessary `Vec<Vec<Nested>>` to write the rep and def levels of `array` to parquet
pub fn to_nested(array: &dyn Array, f: &Field) -> Result<Vec<Vec<Nested>>> {
    let mut nested = vec![];

    to_nested_recursive(array, f, &mut nested, vec![])?;
    Ok(nested)
}

pub fn is_nested_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    )
}

/// Slices the [`Array`] to `ArrayRef` and `Vec<Nested>`.
pub fn slice_nest_array(
    primitive_array: &mut ArrayRef,
    nested: &mut [Nested],
    mut current_offset: usize,
    mut current_length: usize,
) {
    for nested in nested.iter_mut() {
        match nested {
            Nested::LargeList(l_nested) => {
                l_nested.offsets.slice(current_offset, current_length + 1);
                if let Some(nulls) = l_nested.nulls.as_mut() {
                    *nulls = nulls.slice(current_offset, current_length);
                };

                current_length = (*l_nested.offsets.last().unwrap()
                    - *l_nested.offsets.first().unwrap()) as usize;
                current_offset = *l_nested.offsets.first().unwrap() as usize;
            }
            Nested::List(l_nested) => {
                l_nested.offsets.slice(current_offset, current_length + 1);
                if let Some(nulls) = l_nested.nulls.as_mut() {
                    *nulls = nulls.slice(current_offset, current_length);
                };

                current_length = (*l_nested.offsets.last().unwrap()
                    - *l_nested.offsets.first().unwrap()) as usize;
                current_offset = *l_nested.offsets.first().unwrap() as usize;
            }
            Nested::Struct(length, _, nulls) => {
                *length = current_length;
                if let Some(nulls) = nulls.as_mut() {
                    *nulls = nulls.slice(current_offset, current_length);
                };
            }
            Nested::Primitive(length, _, nulls) => {
                *length = current_length;
                if let Some(nulls) = nulls.as_mut() {
                    *nulls = nulls.slice(current_offset, current_length);
                };
                *primitive_array = primitive_array.slice(current_offset, current_length);
            }
        }
    }
}

fn to_nested_recursive(
    array: &dyn Array,
    f: &Field,
    nested: &mut Vec<Vec<Nested>>,
    mut parents: Vec<Nested>,
) -> Result<()> {
    let nullable = f.is_nullable();
    match array.data_type() {
        DataType::Struct(_) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            parents.push(Nested::Struct(
                array.len(),
                nullable,
                array.nulls().cloned(),
            ));

            for (array, f) in array.columns().iter().zip(array.fields().iter()) {
                to_nested_recursive(array.as_ref(), f, nested, parents.clone())?;
            }
        }
        DataType::List(fs) => {
            let array = array.as_any().downcast_ref::<ListArray>().unwrap();
            parents.push(Nested::List(ListNested::new(
                array.offsets().clone(),
                array.nulls().cloned(),
                nullable,
            )));
            to_nested_recursive(array.values().as_ref(), fs.as_ref(), nested, parents)?;
        }
        DataType::LargeList(fs) => {
            let array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            parents.push(Nested::LargeList(ListNested::<i64>::new(
                array.offsets().clone(),
                array.nulls().cloned(),
                nullable,
            )));
            to_nested_recursive(array.values().as_ref(), fs.as_ref(), nested, parents)?;
        }
        DataType::Map(fs, _) => {
            let array = array.as_any().downcast_ref::<MapArray>().unwrap();
            parents.push(Nested::List(ListNested::new(
                array.offsets().clone(),
                array.nulls().cloned(),
                nullable,
            )));
            to_nested_recursive(array.entries(), fs.as_ref(), nested, parents)?;
        }
        _ => {
            parents.push(Nested::Primitive(
                array.len(),
                nullable,
                array.nulls().cloned(),
            ));
            nested.push(parents);
        }
    }
    Ok(())
}

/// Convert [`Array`] to `Vec<&dyn Array>` leaves in DFS order.
pub fn to_leaves(array: &dyn Array) -> Vec<&dyn Array> {
    let mut leaves = vec![];
    to_leaves_recursive(array, &mut leaves);
    leaves
}

fn to_leaves_recursive<'a>(array: &'a dyn Array, leaves: &mut Vec<&'a dyn Array>) {
    use arrow_schema::DataType::*;
    match array.data_type() {
        Struct(_) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array
                .columns()
                .iter()
                .for_each(|a| to_leaves_recursive(a, leaves));
        }
        List(_) => {
            let array = array.as_any().downcast_ref::<ListArray>().unwrap();
            to_leaves_recursive(array.values(), leaves);
        }
        LargeList(_) => {
            let array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            to_leaves_recursive(array.values(), leaves);
        }
        Map(_, _) => {
            let array = array.as_any().downcast_ref::<MapArray>().unwrap();
            to_leaves_recursive(array.entries(), leaves);
        }
        _ => leaves.push(array),
    }
}

/// The initial info of nested data types.
/// The initial info of nested data types.
#[derive(Debug, Clone, Copy)]
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

/// Creates a new [`ListArray`] or [`FixedSizeListArray`].
pub fn create_list(data_type: DataType, nested: &mut NestedState, values: ArrayRef) -> ArrayRef {
    let n = nested.pop().unwrap();
    let (offsets, nulls) = n.inner();
    match data_type {
        DataType::List(f) => {
            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
            Arc::new(ListArray::new(
                f,
                unsafe { OffsetBuffer::new_unchecked(offsets.into()) },
                values,
                nulls.clone(),
            ))
        }
        DataType::LargeList(f) => Arc::new(LargeListArray::new(f, offsets, values, nulls.clone())),
        DataType::FixedSizeList(f, s) => {
            Arc::new(FixedSizeListArray::new(f, s, values, nulls.clone()))
        }
        _ => unreachable!(),
    }
}

/// Creates a new [`MapArray`].
pub fn create_map(data_type: DataType, nested: &mut NestedState, values: ArrayRef) -> ArrayRef {
    let n = nested.pop().unwrap();
    let (offsets, nulls) = n.inner();
    match data_type {
        DataType::Map(fs, _) => {
            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
            let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };

            let values = values.as_any().downcast_ref::<StructArray>().unwrap();
            Arc::new(MapArray::new(
                fs,
                offsets,
                values.clone(),
                nulls.clone(),
                false,
            ))
        }
        _ => unreachable!(),
    }
}

pub fn create_struct(
    fields: Vec<Field>,
    nested: &mut Vec<NestedState>,
    values: Vec<ArrayRef>,
) -> (NestedState, ArrayRef) {
    let mut nest = nested.pop().unwrap();
    let n = nest.pop().unwrap();
    let (_, nulls) = n.inner();

    (
        nest,
        Arc::new(StructArray::new(fields.into(), values, nulls.clone())),
    )
}
