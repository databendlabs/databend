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
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::marker::Send;
use std::marker::Sync;

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_exception::Result;
use common_expression::types::number::Number;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::ValueType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_hashtable::HashSet as CommonHashSet;
use common_hashtable::HashSetWithStackMemory;
use common_hashtable::HashTableEntity;
use common_hashtable::HashTableKeyable;
use common_hashtable::KeysRef;
use common_io::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait DistinctStateFunc<S>: Send + Sync {
    fn new() -> Self;
    fn serialize(&self, writer: &mut BytesMut) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn add(&mut self, columns: &[Column], row: usize) -> Result<()>;
    fn batch_add(
        &mut self,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn build_columns(&mut self, types: &[DataType]) -> Result<Vec<Column>>;
}

pub struct AggregateDistinctState {
    set: HashSet<Vec<u8>, RandomState>,
}

pub struct AggregateDistinctNumberState<T: Number + HashTableKeyable> {
    set: HashSetWithStackMemory<{ 16 * 8 }, T>,
    inserted: bool,
}

const HOLDER_CAPACITY: usize = 256;
const HOLDER_BYTES_CAPACITY: usize = HOLDER_CAPACITY * 8;

pub struct AggregateDistinctStringState {
    set: CommonHashSet<KeysRef>,
    inserted: bool,
    holders: Vec<StringColumnBuilder>,
}

pub struct DataGroupValue;

impl DistinctStateFunc<DataGroupValue> for AggregateDistinctState {
    fn new() -> Self {
        AggregateDistinctState {
            set: HashSet::new(),
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, &self.set)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.set = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: &[Column], row: usize) -> Result<()> {
        let values = columns
            .iter()
            .map(|col| unsafe { AnyType::index_column_unchecked(col, row).to_owned() })
            .collect::<Vec<_>>();
        let mut buffer = Vec::with_capacity(values.len() * std::mem::size_of::<Scalar>());
        serialize_into_buf(&mut buffer, &values)?;
        self.set.insert(buffer);
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        for row in 0..input_rows {
            if validity.map(|v| v.get_bit(row)).unwrap_or(true) {
                let values = columns
                    .iter()
                    .map(|col| unsafe { AnyType::index_column_unchecked(col, row).to_owned() })
                    .collect::<Vec<_>>();

                let mut buffer = Vec::with_capacity(values.len() * std::mem::size_of::<Scalar>());
                serialize_into_buf(&mut buffer, &values)?;
            }
        }
        Ok(())
    }
    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.extend(rhs.set.clone());
        Ok(())
    }

    fn build_columns(&mut self, types: &[DataType]) -> Result<Vec<Column>> {
        let mut builders: Vec<ColumnBuilder> = types
            .iter()
            .map(|ty| ColumnBuilder::with_capacity(ty, self.set.len()))
            .collect();

        for data in self.set.iter() {
            let mut slice = data.as_slice();
            let scalars: Vec<Scalar> = deserialize_from_slice(&mut slice)?;
            scalars.iter().enumerate().for_each(|(idx, group_value)| {
                builders[idx].push(group_value.as_ref());
            });
        }

        Ok(builders.into_iter().map(|b| b.build()).collect())
    }
}

impl AggregateDistinctStringState {
    #[inline]
    fn insert_and_materialize(&mut self, key: &KeysRef) {
        let entity = self.set.insert_key(key, &mut self.inserted);
        if self.inserted {
            let data = unsafe { key.as_slice() };

            let holder = self.holders.last_mut().unwrap();
            // TODO(sundy): may cause memory fragmentation, refactor this using arena
            if holder.may_resize(data.len()) {
                let mut holder = StringColumnBuilder::with_capacity(
                    HOLDER_CAPACITY,
                    HOLDER_BYTES_CAPACITY.max(data.len()),
                );
                holder.put_slice(data);
                holder.commit_row();
                let value = unsafe { holder.index_unchecked(holder.len() - 1) };
                entity.set_key(KeysRef::create(value.as_ptr() as usize, value.len()));
                self.holders.push(holder);
            } else {
                holder.put_slice(data);
                holder.commit_row();
                let value = unsafe { holder.index_unchecked(holder.len() - 1) };
                entity.set_key(KeysRef::create(value.as_ptr() as usize, value.len()));
            }
        }
    }
}

impl DistinctStateFunc<KeysRef> for AggregateDistinctStringState {
    fn new() -> Self {
        AggregateDistinctStringState {
            set: CommonHashSet::create(),
            inserted: false,
            holders: vec![StringColumnBuilder::with_capacity(
                HOLDER_CAPACITY,
                HOLDER_BYTES_CAPACITY,
            )],
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, &self.holders)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.holders = deserialize_from_slice(reader)?;
        self.set = CommonHashSet::with_capacity(self.holders.iter().map(|h| h.len()).sum());

        for holder in self.holders.iter() {
            for index in 0..holder.len() {
                let data = unsafe { holder.index_unchecked(index) };
                let key = KeysRef::create(data.as_ptr() as usize, data.len());
                self.set.insert_key(&key, &mut self.inserted);
            }
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: &[Column], row: usize) -> Result<()> {
        let column = StringType::try_downcast_column(&columns[0]).unwrap();
        let data = unsafe { column.index_unchecked(row) };
        let key = KeysRef::create(data.as_ptr() as usize, data.len());
        self.insert_and_materialize(&key);
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let column = StringType::try_downcast_column(&columns[0]).unwrap();

        match validity {
            Some(v) => {
                for row in 0..input_rows {
                    if v.get_bit(row) {
                        let data = unsafe { column.index_unchecked(row) };
                        let key = KeysRef::create(data.as_ptr() as usize, data.len());
                        self.insert_and_materialize(&key);
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let data = unsafe { column.index_unchecked(row) };
                    let key = KeysRef::create(data.as_ptr() as usize, data.len());
                    self.insert_and_materialize(&key);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        for value in rhs.set.iter() {
            self.insert_and_materialize(value.get_key());
        }
        Ok(())
    }

    fn build_columns(&mut self, _types: &[DataType]) -> Result<Vec<Column>> {
        if self.holders.len() == 1 {
            let c = std::mem::replace(
                &mut self.holders[0],
                StringColumnBuilder::with_capacity(0, 0),
            );
            return Ok(vec![Column::String(c.build())]);
        }

        let mut values = Vec::with_capacity(self.holders.iter().map(|h| h.data.len()).sum());
        let mut offsets = Vec::with_capacity(self.holders.iter().map(|h| h.len()).sum());

        let mut last_offset = 0;
        offsets.push(0);
        for holder in self.holders.iter_mut() {
            for offset in holder.offsets.iter() {
                last_offset += *offset;
                offsets.push(last_offset);
            }
            values.append(&mut holder.data);
        }
        let c = StringColumnBuilder {
            data: values,
            offsets,
        };
        Ok(vec![Column::String(c.build())])
    }
}

impl<T> DistinctStateFunc<T> for AggregateDistinctNumberState<T>
where T: Number + Serialize + DeserializeOwned + HashTableKeyable
{
    fn new() -> Self {
        AggregateDistinctNumberState {
            set: HashSetWithStackMemory::create(),
            inserted: false,
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for value in self.set.iter() {
            serialize_into_buf(writer, value.get_key())?
        }
        Ok(())
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        let size = reader.read_uvarint()?;
        self.set = HashSetWithStackMemory::with_capacity(size as usize);
        for _ in 0..size {
            let t: T = deserialize_from_slice(reader)?;
            let _ = self.set.insert_key(&t, &mut self.inserted);
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: &[Column], row: usize) -> Result<()> {
        let col = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        let v = unsafe { col.get_unchecked(row) };
        self.set.insert_key(v, &mut self.inserted);
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let col = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        match validity {
            Some(bitmap) => {
                for (t, v) in col.iter().zip(bitmap.iter()) {
                    if v {
                        self.set.insert_key(t, &mut self.inserted);
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let v = unsafe { col.get_unchecked(row) };
                    self.set.insert_key(v, &mut self.inserted);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.merge(&rhs.set);
        Ok(())
    }

    fn build_columns(&mut self, _types: &[DataType]) -> Result<Vec<Column>> {
        let values: Buffer<T> = self.set.iter().map(|e| *e.get_key()).collect();
        Ok(vec![NumberType::<T>::upcast_column(values)])
    }
}
