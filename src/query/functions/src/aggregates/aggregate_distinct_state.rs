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

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::hash::Hasher;
use std::io::BufRead;
use std::marker::Send;
use std::marker::Sync;
use std::sync::Arc;

use bumpalo::Bump;
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
use common_hashtable::HashtableKeyable;
use common_hashtable::HashtableLike;
use common_hashtable::ShortStringHashSet;
use common_hashtable::StackHashSet;
use common_io::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use super::deserialize_state;
use super::serialize_state;

pub trait DistinctStateFunc: Sized + Send + Sync {
    fn new() -> Self;
    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()>;
    fn deserialize(reader: &mut &[u8]) -> Result<Self>;
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

// Tried to use StackHash<T, 4> but performance is improved in Q14 of hits benchmark
pub struct AggregateDistinctNumberState<T: Number + HashtableKeyable> {
    set: CommonHashSet<T>,
}

pub struct AggregateDistinctStringState {
    set: ShortStringHashSet<[u8]>,
}

impl DistinctStateFunc for AggregateDistinctState {
    fn new() -> Self {
        AggregateDistinctState {
            set: HashSet::new(),
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.set)
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        let set = deserialize_state(reader)?;
        Ok(Self { set })
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
        serialize_state(&mut buffer, &values)?;
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
                serialize_state(&mut buffer, &values)?;
                self.set.insert(buffer);
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
            let scalars: Vec<Scalar> = deserialize_state(&mut slice)?;
            scalars.iter().enumerate().for_each(|(idx, group_value)| {
                builders[idx].push(group_value.as_ref());
            });
        }

        Ok(builders.into_iter().map(|b| b.build()).collect())
    }
}

impl DistinctStateFunc for AggregateDistinctStringState {
    fn new() -> Self {
        AggregateDistinctStringState {
            set: ShortStringHashSet::<[u8]>::with_capacity(4, Arc::new(Bump::new())),
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for k in self.set.iter() {
            writer.write_binary(k.key())?;
        }
        Ok(())
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        let size = reader.read_uvarint()?;
        let mut set =
            ShortStringHashSet::<[u8]>::with_capacity(size as usize, Arc::new(Bump::new()));
        for _ in 0..size {
            let s = reader.read_uvarint()? as usize;
            let _ = set.set_insert(&reader[..s]);
            reader.consume(s);
        }
        Ok(Self { set })
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
        let _ = self.set.set_insert(data);
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
                        let _ = self.set.set_insert(data);
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let data = unsafe { column.index_unchecked(row) };
                    let _ = self.set.set_insert(data);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    fn build_columns(&mut self, _types: &[DataType]) -> Result<Vec<Column>> {
        let mut builder = StringColumnBuilder::with_capacity(self.set.len(), self.set.len() * 2);
        for key in self.set.iter() {
            builder.put_slice(key.key());
            builder.commit_row();
        }
        Ok(vec![Column::String(builder.build())])
    }
}

impl<T> DistinctStateFunc for AggregateDistinctNumberState<T>
where T: Number + Serialize + DeserializeOwned + HashtableKeyable
{
    fn new() -> Self {
        AggregateDistinctNumberState {
            set: CommonHashSet::with_capacity(4),
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for e in self.set.iter() {
            serialize_state(writer, e.key())?
        }
        Ok(())
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        let size = reader.read_uvarint()?;
        let mut set = CommonHashSet::with_capacity(size as usize);
        for _ in 0..size {
            let t: T = deserialize_state(reader)?;
            let _ = set.set_insert(t).is_ok();
        }
        Ok(Self { set })
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
        let _ = self.set.set_insert(*v).is_ok();
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
                        let _ = self.set.set_insert(*t).is_ok();
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let v = unsafe { col.get_unchecked(row) };
                    let _ = self.set.set_insert(*v).is_ok();
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    fn build_columns(&mut self, _types: &[DataType]) -> Result<Vec<Column>> {
        let values: Buffer<T> = self.set.iter().map(|e| *e.key()).collect();
        Ok(vec![NumberType::<T>::upcast_column(values)])
    }
}

// For count(distinct string) and uniq(string)
pub struct AggregateUniqStringState {
    set: StackHashSet<u128, 16>,
}

impl DistinctStateFunc for AggregateUniqStringState {
    fn new() -> Self {
        AggregateUniqStringState {
            set: StackHashSet::new(),
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for value in self.set.iter() {
            serialize_state(writer, value.key())?
        }
        Ok(())
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        let size = reader.read_uvarint()?;
        let mut set = StackHashSet::with_capacity(size as usize);
        for _ in 0..size {
            let e = deserialize_state(reader)?;
            let _ = set.set_insert(e).is_ok();
        }
        Ok(Self { set })
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: &[Column], row: usize) -> Result<()> {
        let column = columns[0].as_string().unwrap();
        let data = unsafe { column.index_unchecked(row) };
        let mut hasher = SipHasher24::new();
        hasher.write(data);
        let hash128 = hasher.finish128();
        let _ = self.set.set_insert(hash128.into()).is_ok();
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let column = columns[0].as_string().unwrap();
        match validity {
            Some(v) => {
                for (t, v) in column.iter().zip(v.iter()) {
                    if v {
                        let mut hasher = SipHasher24::new();
                        hasher.write(t);
                        let hash128 = hasher.finish128();
                        let _ = self.set.set_insert(hash128.into()).is_ok();
                    }
                }
            }
            _ => {
                for row in 0..input_rows {
                    let data = unsafe { column.index_unchecked(row) };
                    let mut hasher = SipHasher24::new();
                    hasher.write(data);
                    let hash128 = hasher.finish128();
                    let _ = self.set.set_insert(hash128.into()).is_ok();
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    // This method won't be called.
    fn build_columns(&mut self, _types: &[DataType]) -> Result<Vec<Column>> {
        Ok(vec![])
    }
}
