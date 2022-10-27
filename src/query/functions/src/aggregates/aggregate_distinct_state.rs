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
use std::hash::Hasher;
use std::marker::PhantomData;
use std::marker::Send;
use std::marker::Sync;

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_hashtable::HashSet as CommonHashSet;
use common_hashtable::HashtableKeyable;
use common_hashtable::KeysRef;
use common_hashtable::StackHashSet;
use common_io::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

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

pub struct AggregateDistinctPrimitiveState<T: PrimitiveType, E: From<T> + HashtableKeyable> {
    set: StackHashSet<E, 16>,
    _t: PhantomData<T>,
}

const HOLDER_CAPACITY: usize = 256;
const HOLDER_BYTES_CAPACITY: usize = HOLDER_CAPACITY * 8;

pub struct AggregateDistinctStringState {
    set: CommonHashSet<KeysRef>,
    inserted: bool,
    holders: Vec<MutableStringColumn>,
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
        match validity {
            Some(v) => {
                for row in 0..input_rows {
                    if v.get_bit(row) {
                        let values = columns.iter().map(|s| s.get(row)).collect::<Vec<_>>();
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
            _ => {
                for row in 0..input_rows {
                    let values = columns.iter().map(|s| s.get(row)).collect::<Vec<_>>();
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

impl AggregateDistinctStringState {
    #[inline]
    fn insert_and_materialize(&mut self, key: &KeysRef) {
        match unsafe { self.set.insert_and_entry(*key) } {
            Ok(entity) => {
                self.inserted = true;
                let data = unsafe { key.as_slice() };

                let holder = self.holders.last_mut().unwrap();
                // TODO(sundy): may cause memory fragmentation, refactor this using arena
                if holder.may_resize(data.len()) {
                    let mut holder = MutableStringColumn::with_values_capacity(
                        HOLDER_BYTES_CAPACITY.max(data.len()),
                        HOLDER_CAPACITY,
                    );
                    holder.push(data);
                    let value = unsafe { holder.value_unchecked(holder.len() - 1) };
                    unsafe {
                        entity.set_key(KeysRef::create(value.as_ptr() as usize, value.len()));
                    }
                    self.holders.push(holder);
                } else {
                    holder.push(data);
                    let value = unsafe { holder.value_unchecked(holder.len() - 1) };
                    unsafe {
                        entity.set_key(KeysRef::create(value.as_ptr() as usize, value.len()));
                    }
                }
            }
            Err(_) => {
                self.inserted = false;
            }
        }
    }
}

impl DistinctStateFunc<KeysRef> for AggregateDistinctStringState {
    fn new() -> Self {
        AggregateDistinctStringState {
            set: CommonHashSet::new(),
            inserted: false,
            holders: vec![MutableStringColumn::with_values_capacity(
                HOLDER_BYTES_CAPACITY,
                HOLDER_CAPACITY,
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
                let data = unsafe { holder.value_unchecked(index) };
                let key = KeysRef::create(data.as_ptr() as usize, data.len());
                self.inserted = self.set.set_insert(key).is_ok();
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

    fn add(&mut self, columns: &[ColumnRef], row: usize) -> Result<()> {
        let column: &StringColumn = unsafe { Series::static_cast(&columns[0]) };
        let data = column.get_data(row);
        let key = KeysRef::create(data.as_ptr() as usize, data.len());
        self.insert_and_materialize(&key);
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let column: &StringColumn = unsafe { Series::static_cast(&columns[0]) };

        match validity {
            Some(v) => {
                for row in 0..input_rows {
                    if v.get_bit(row) {
                        let data = column.get_data(row);
                        let key = KeysRef::create(data.as_ptr() as usize, data.len());
                        self.insert_and_materialize(&key);
                    }
                }
            }
            _ => {
                for row in 0..input_rows {
                    let data = column.get_data(row);
                    let key = KeysRef::create(data.as_ptr() as usize, data.len());
                    self.insert_and_materialize(&key);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        for value in rhs.set.iter() {
            self.insert_and_materialize(value.key());
        }
        Ok(())
    }

    // After build_columns, the set is not avaiable any more.
    fn build_columns(&mut self, _fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        if self.holders.len() == 1 {
            let c = self.holders[0].finish();
            return Ok(vec![c.arc()]);
        }
        let mut values = Vec::with_capacity(
            self.holders
                .iter()
                .map(|h| h.values_offsets().0.len())
                .sum(),
        );
        let mut offsets = Vec::with_capacity(self.holders.iter().map(|h| h.len()).sum());

        let mut last_offset = 0;
        offsets.push(0);
        for holder in self.holders.iter_mut() {
            for offset in holder.values_offsets().1.iter() {
                last_offset += *offset;
                offsets.push(last_offset);
            }
            values.append(holder.values_mut());
        }
        let mut c = MutableStringColumn::from_data(values, offsets);
        Ok(vec![c.finish().arc()])
    }
}

impl<T, E> DistinctStateFunc<T> for AggregateDistinctPrimitiveState<T, E>
where
    T: PrimitiveType + From<E>,
    E: From<T> + Sync + Send + Clone + std::fmt::Debug + HashtableKeyable,
{
    fn new() -> Self {
        AggregateDistinctPrimitiveState {
            set: StackHashSet::new(),
            _t: PhantomData,
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for value in self.set.iter() {
            let t: T = (*value.key()).into();
            serialize_into_buf(writer, &t)?
        }
        Ok(())
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        let size = reader.read_uvarint()?;
        self.set = StackHashSet::with_capacity(size as usize);
        for _ in 0..size {
            let t: T = deserialize_from_slice(reader)?;
            let e = E::from(t);
            let _ = self.set.set_insert(e);
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
        let _ = self.set.set_insert(E::from(v));
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
                        let _ = self.set.set_insert(E::from(*t));
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let v = unsafe { array.value_unchecked(row) };
                    let _ = self.set.set_insert(E::from(v));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        for x in rhs.set.iter() {
            let _ = self.set.set_insert(*x.key());
        }
        Ok(())
    }

    fn build_columns(&mut self, _fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        let values: Vec<T> = self.set.iter().map(|e| (*e.key()).into()).collect();
        let result = PrimitiveColumn::<T>::new_from_vec(values);
        Ok(vec![result.arc()])
    }
}

// For count(distinct string) and uniq(string)
pub struct AggregateUniqStringState {
    set: StackHashSet<u128, 16>,
    inserted: bool,
}

impl DistinctStateFunc<u128> for AggregateUniqStringState {
    fn new() -> Self {
        AggregateUniqStringState {
            set: StackHashSet::new(),
            inserted: false,
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for value in self.set.iter() {
            serialize_into_buf(writer, value.key())?
        }
        Ok(())
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        let size = reader.read_uvarint()?;
        self.set = StackHashSet::with_capacity(size as usize);
        for _ in 0..size {
            let e = deserialize_from_slice(reader)?;
            self.inserted = self.set.set_insert(e).is_ok();
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
        let mut hasher = SipHasher24::new();
        hasher.write(data);
        let hash128 = hasher.finish128();
        self.inserted = self.set.set_insert(hash128.into()).is_ok();
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let column: &StringColumn = unsafe { Series::static_cast(&columns[0]) };
        match validity {
            Some(v) => {
                for (t, v) in column.iter().zip(v.iter()) {
                    if v {
                        let mut hasher = SipHasher24::new();
                        hasher.write(t);
                        let hash128 = hasher.finish128();
                        self.inserted = self.set.set_insert(hash128.into()).is_ok();
                    }
                }
            }
            _ => {
                for row in 0..input_rows {
                    let data = column.get_data(row);
                    let mut hasher = SipHasher24::new();
                    hasher.write(data);
                    let hash128 = hasher.finish128();
                    self.inserted = self.set.set_insert(hash128.into()).is_ok();
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
    fn build_columns(&mut self, _fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        Ok(vec![])
    }
}
