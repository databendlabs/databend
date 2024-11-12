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

use crate::arrow::array::Array;
use crate::arrow::array::FixedSizeListArray;
use crate::arrow::array::ListArray;
use crate::arrow::array::MapArray;
use crate::arrow::array::StructArray;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Field;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::offset::Offset;
use crate::arrow::offset::Offsets;
use crate::arrow::offset::OffsetsBuffer;

/// Descriptor of nested information of a field
#[derive(Debug, Clone, PartialEq)]
pub enum Nested {
    /// A primitive array
    Primitive(usize, bool, Option<Bitmap>),
    /// a list
    List(ListNested<i32>),
    /// a list
    LargeList(ListNested<i64>),
    /// A struct array
    Struct(usize, bool, Option<Bitmap>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListNested<O: Offset> {
    pub is_nullable: bool,
    pub offsets: OffsetsBuffer<O>,
    pub validity: Option<Bitmap>,
}

impl<O: Offset> ListNested<O> {
    pub fn new(offsets: OffsetsBuffer<O>, validity: Option<Bitmap>, is_nullable: bool) -> Self {
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
            Nested::List(l) => l.offsets.len_proxy(),
            Nested::LargeList(l) => l.offsets.len_proxy(),
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

    pub fn inner(&self) -> (Buffer<i64>, &Option<Bitmap>) {
        match self {
            Nested::Primitive(_, _, v) => (Buffer::new(), v),
            Nested::List(l) => {
                let start = l.offsets.first();
                let buffer = l
                    .offsets
                    .buffer()
                    .iter()
                    .map(|x| (*x - start) as i64)
                    .collect();
                (buffer, &l.validity)
            }
            Nested::LargeList(l) => {
                let start = l.offsets.first();
                let buffer = if *start == 0 {
                    l.offsets.buffer().clone()
                } else {
                    l.offsets.buffer().iter().map(|x| *x - start).collect()
                };
                (buffer, &l.validity)
            }
            Nested::Struct(_, _, v) => (Buffer::new(), v),
        }
    }

    pub fn validity(&self) -> &Option<Bitmap> {
        match self {
            Nested::Primitive(_, _, v) => v,
            Nested::List(l) => &l.validity,
            Nested::LargeList(l) => &l.validity,
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

/// Slices the [`Array`] to `Box<dyn Array>` and `Vec<Nested>`.
pub fn slice_nest_array(
    primitive_array: &mut dyn Array,
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

                current_length = l_nested.offsets.range() as usize;
                current_offset = *l_nested.offsets.first() as usize;
            }
            Nested::List(l_nested) => {
                l_nested.offsets.slice(current_offset, current_length + 1);
                if let Some(validity) = l_nested.validity.as_mut() {
                    validity.slice(current_offset, current_length)
                };

                current_length = l_nested.offsets.range() as usize;
                current_offset = *l_nested.offsets.first() as usize;
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
                primitive_array.slice(current_offset, current_length);
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
    use PhysicalType::*;
    let lt = f.data_type.to_logical_type();
    let nullable = f.is_nullable;
    match array.data_type().to_physical_type() {
        Struct => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            parents.push(Nested::Struct(
                array.len(),
                nullable,
                array.validity().cloned(),
            ));

            if let DataType::Struct(fs) = lt {
                for (array, f) in array.values().iter().zip(fs.iter()) {
                    to_nested_recursive(array.as_ref(), f, nested, parents.clone())?;
                }
            } else {
                return Err(Error::InvalidArgumentError(
                    "DataType type must be a group for a struct array".to_string(),
                ));
            }
        }
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();

            if let DataType::List(fs) = lt {
                parents.push(Nested::List(ListNested::new(
                    array.offsets().clone(),
                    array.validity().cloned(),
                    nullable,
                )));
                to_nested_recursive(array.values().as_ref(), fs.as_ref(), nested, parents)?;
            } else {
                return Err(Error::InvalidArgumentError(
                    "DataType type must be a group for a List array".to_string(),
                ));
            }
        }
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            if let DataType::LargeList(fs) = lt {
                parents.push(Nested::LargeList(ListNested::<i64>::new(
                    array.offsets().clone(),
                    array.validity().cloned(),
                    nullable,
                )));
                to_nested_recursive(array.values().as_ref(), fs.as_ref(), nested, parents)?;
            } else {
                return Err(Error::InvalidArgumentError(
                    "DataType type must be a group for a LargeList array".to_string(),
                ));
            }
        }
        Map => {
            let array = array.as_any().downcast_ref::<MapArray>().unwrap();
            if let DataType::Map(fs, _) = lt {
                parents.push(Nested::List(ListNested::new(
                    array.offsets().clone(),
                    array.validity().cloned(),
                    nullable,
                )));
                to_nested_recursive(array.field().as_ref(), fs.as_ref(), nested, parents)?;
            } else {
                return Err(Error::InvalidArgumentError(
                    "DataType type must be a group for a LargeList array".to_string(),
                ));
            }
        }
        _ => {
            parents.push(Nested::Primitive(
                array.len(),
                nullable,
                array.validity().cloned(),
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
    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Struct => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array
                .values()
                .iter()
                .for_each(|a| to_leaves_recursive(a.as_ref(), leaves));
        }
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        }
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        }
        Map => {
            let array = array.as_any().downcast_ref::<MapArray>().unwrap();
            to_leaves_recursive(array.field().as_ref(), leaves);
        }
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | LargeUtf8 | Dictionary(_) | BinaryView | Utf8View => leaves.push(array),
        other => todo!("Writing {:?} to native not yet implemented", other),
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

/// Creates a new [`ListArray`] or [`FixedSizeListArray`].
pub fn create_list(
    data_type: DataType,
    nested: &mut NestedState,
    values: Box<dyn Array>,
) -> Box<dyn Array> {
    let n = nested.pop().unwrap();
    let (offsets, validity) = n.inner();
    match data_type.to_logical_type() {
        DataType::List(_) => {
            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
            let offsets: Offsets<i32> = offsets
                .try_into()
                .expect("i64 offsets do not fit in i32 offsets");

            Box::new(ListArray::<i32>::new(
                data_type,
                OffsetsBuffer::from(offsets),
                values,
                validity.clone(),
            ))
        }
        DataType::LargeList(_) => Box::new(ListArray::<i64>::new(
            data_type,
            unsafe { OffsetsBuffer::new_unchecked(offsets) },
            values,
            validity.clone(),
        )),
        DataType::FixedSizeList(_, _) => {
            Box::new(FixedSizeListArray::new(data_type, values, validity.clone()))
        }
        _ => unreachable!(),
    }
}

/// Creates a new [`MapArray`].
pub fn create_map(
    data_type: DataType,
    nested: &mut NestedState,
    values: Box<dyn Array>,
) -> Box<dyn Array> {
    let n = nested.pop().unwrap();
    let (offsets, validity) = n.inner();
    match data_type.to_logical_type() {
        DataType::Map(_, _) => {
            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();

            let offsets: Offsets<i32> = offsets
                .try_into()
                .expect("i64 offsets do not fit in i32 offsets");

            Box::new(MapArray::new(
                data_type,
                offsets.into(),
                values,
                validity.clone(),
            ))
        }
        _ => unreachable!(),
    }
}

pub fn create_struct(
    fields: Vec<Field>,
    nested: &mut Vec<NestedState>,
    values: Vec<Box<dyn Array>>,
) -> (NestedState, Box<dyn Array>) {
    let mut nest = nested.pop().unwrap();
    let n = nest.pop().unwrap();
    let (_, validity) = n.inner();

    (
        nest,
        Box::new(StructArray::new(
            DataType::Struct(fields),
            values,
            validity.clone(),
        )),
    )
}
