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
use std::convert::TryFrom;
use std::hash::Hash;
use std::marker::PhantomData;
use std::marker::Send;
use std::marker::Sync;

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_hashtable::HashSetWithStackMemory;
use common_hashtable::HashTableEntity;
use common_hashtable::HashTableKeyable;
use common_hashtable::KeysRef;
use common_io::prelude::*;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_distinct_state::DataGroupValue;

pub trait DistinctStateFunc<S>: Send + Sync {
    fn new() -> Self;
    fn serialize(&self, writer: &mut BytesMut) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn add(&mut self, columns: &[ColumnRef], row: usize) -> Result<()>;
    fn batch_add(
        &mut self,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn build_columns(&mut self, fields: &[DataField]) -> Result<Vec<ColumnRef>>;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct DataGroupValues(Vec<DataGroupValue>);

pub struct AggregateDistinctState {
    set: HashSet<DataGroupValues, RandomState>,
}

pub struct AggregateDistinctPrimitiveState<T: PrimitiveType, E: From<T> + HashTableKeyable> {
    set: HashSetWithStackMemory<{ 16 * 8 }, E>,
    _t: PhantomData<T>,
    inserted: bool,
}

pub struct AggregateDistinctStringState {
    set: HashSet<KeysRef, RandomState>,
    holder: MutableStringColumn,
}

impl DistinctStateFunc<DataGroupValues> for AggregateDistinctState {
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

    fn add(&mut self, columns: &[ColumnRef], row: usize) -> Result<()> {
        let values = columns.iter().map(|s| s.get(row)).collect::<Vec<_>>();
        let data_values = DataGroupValues(
            values
                .iter()
                .map(DataGroupValue::try_from)
                .collect::<Result<Vec<_>>>()?,
        );
        self.set.insert(data_values);
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        for row in 0..input_rows {
            let values = columns.iter().map(|s| s.get(row)).collect::<Vec<_>>();
            match validity {
                Some(v) => {
                    if v.get_bit(row) {
                        let data_values = DataGroupValues(
                            values
                                .iter()
                                .map(DataGroupValue::try_from)
                                .collect::<Result<Vec<_>>>()?,
                        );
                        self.set.insert(data_values);
                    }
                }
                None => {
                    let data_values = DataGroupValues(
                        values
                            .iter()
                            .map(DataGroupValue::try_from)
                            .collect::<Result<Vec<_>>>()?,
                    );
                    self.set.insert(data_values);
                }
            }
        }
        Ok(())
    }
    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.extend(rhs.set.clone());
        Ok(())
    }

    fn build_columns(&mut self, fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        let mut results = Vec::with_capacity(self.set.len());
        self.set.iter().for_each(|group_values| {
            let mut v = Vec::with_capacity(group_values.0.len());
            group_values.0.iter().for_each(|group_value| {
                v.push(DataValue::from(group_value));
            });

            results.push(v);
        });
        let results = (0..fields.len())
            .map(|i| {
                results
                    .iter()
                    .map(|inner| inner[i].clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        results
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let data_type = fields[i].data_type();
                data_type.create_column(v)
            })
            .collect::<Result<Vec<_>>>()
    }
}

impl DistinctStateFunc<KeysRef> for AggregateDistinctStringState {
    fn new() -> Self {
        AggregateDistinctStringState {
            set: HashSet::new(),
            holder: MutableStringColumn::with_capacity(0),
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        let (values, offsets) = self.holder.values_offsets();
        serialize_into_buf(writer, offsets)?;
        serialize_into_buf(writer, values)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        let offsets: Vec<i64> = deserialize_from_slice(reader)?;
        let values: Vec<u8> = deserialize_from_slice(reader)?;

        self.holder = MutableStringColumn::from_data(values, offsets);
        self.set = HashSet::with_capacity(self.holder.len());

        for index in 0..self.holder.len() {
            let data = unsafe { self.holder.value_unchecked(index) };
            let key = KeysRef::create(data.as_ptr() as usize, data.len());
            self.set.insert(key);
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: &[ColumnRef], row: usize) -> Result<()> {
        let column: &StringColumn = unsafe { Series::static_cast(&columns[0]) };
        let data = column.get_data(row);
        let mut key = KeysRef::create(data.as_ptr() as usize, data.len());

        if !self.set.contains(&key) {
            self.holder.push(data);
            let data = unsafe { self.holder.value_unchecked(self.holder.len() - 1) };
            key = KeysRef::create(data.as_ptr() as usize, data.len());
            self.set.insert(key);
        }
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let column: &StringColumn = unsafe { Series::static_cast(&columns[0]) };

        for row in 0..input_rows {
            match validity {
                Some(v) => {
                    if v.get_bit(row) {
                        let data = column.get_data(row);
                        let mut key = KeysRef::create(data.as_ptr() as usize, data.len());
                        if !self.set.contains(&key) {
                            self.holder.push(data);
                            let data =
                                unsafe { self.holder.value_unchecked(self.holder.len() - 1) };
                            key = KeysRef::create(data.as_ptr() as usize, data.len());
                            self.set.insert(key);
                        }
                    }
                }
                None => {
                    let data = column.get_data(row);
                    let mut key = KeysRef::create(data.as_ptr() as usize, data.len());
                    if !self.set.contains(&key) {
                        self.holder.push(data);
                        let data = unsafe { self.holder.value_unchecked(self.holder.len() - 1) };
                        key = KeysRef::create(data.as_ptr() as usize, data.len());
                        self.set.insert(key);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.extend(rhs.set.clone());
        Ok(())
    }

    fn build_columns(&mut self, _fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        let c = self.holder.finish();
        Ok(vec![c.arc()])
    }
}

impl<T, E> DistinctStateFunc<T> for AggregateDistinctPrimitiveState<T, E>
where
    T: PrimitiveType + From<E>,
    E: From<T> + Sync + Send + Clone + std::fmt::Debug + HashTableKeyable,
{
    fn new() -> Self {
        AggregateDistinctPrimitiveState {
            set: HashSetWithStackMemory::create(),
            _t: PhantomData,
            inserted: false,
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for value in self.set.iter() {
            let t: T = value.get_key().clone().into();
            serialize_into_buf(writer, &t)?
        }
        Ok(())
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        let size = reader.read_uvarint()?;
        self.set = HashSetWithStackMemory::with_capacity(size as usize);
        for _ in 0..size {
            let t: T = deserialize_from_slice(reader)?;
            let e = E::from(t);
            let _ = self.set.insert_key(&e, &mut self.inserted);
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: &[ColumnRef], row: usize) -> Result<()> {
        let array: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        let v = unsafe { array.value_unchecked(row) };
        self.set.insert_key(&E::from(v), &mut self.inserted);
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let array: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        match validity {
            Some(bitmap) => {
                for (t, v) in array.iter().zip(bitmap.iter()) {
                    if v {
                        self.set.insert_key(&E::from(*t), &mut self.inserted);
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let v = unsafe { array.value_unchecked(row) };
                    self.set.insert_key(&E::from(v), &mut self.inserted);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.merge(&rhs.set);
        Ok(())
    }

    fn build_columns(&mut self, _fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        let values: Vec<T> = self
            .set
            .iter()
            .map(|e| e.get_key().clone().into())
            .collect();
        let result = PrimitiveColumn::<T>::new_from_vec(values);
        Ok(vec![result.arc()])
    }
}
