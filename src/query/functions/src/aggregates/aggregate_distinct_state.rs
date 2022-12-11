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

use common_arrow::arrow::bitmap::Bitmap;
use common_base::runtime::ThreadPool;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::FastHash;
use common_hashtable::HashSet as CommonHashSet;
use common_hashtable::HashtableKeyable;
use common_hashtable::HashtableLike;
use common_hashtable::StackHashSet;
use common_hashtable::TwoLevelHashSet;
use common_hashtable::UnsizedHashSet;
use common_io::prelude::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use super::aggregate_distinct_state::DataGroupValue;

pub trait DistinctStateFunc: Send + Sync {
    fn new() -> Self;
    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()>;
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

    fn support_merge_parallel() -> bool {
        false
    }

    fn merge_parallel(&mut self, _pool: &mut ThreadPool, _rhs: &mut Self) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "merge_parallel is not implemented".to_string(),
        ))
    }

    fn build_columns(&mut self, fields: &[DataField]) -> Result<Vec<ColumnRef>>;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct DataGroupValues(Vec<DataGroupValue>);

pub struct AggregateDistinctState {
    set: HashSet<DataGroupValues, RandomState>,
}

pub struct AggregateDistinctPrimitiveState<T: PrimitiveType, E: From<T> + HashtableKeyable> {
    set: CommonHashSet<E>,
    _t: PhantomData<T>,
}

pub struct AggregateDistinctStringState {
    set: UnsizedHashSet<[u8]>,
}

impl DistinctStateFunc for AggregateDistinctState {
    fn new() -> Self {
        AggregateDistinctState {
            set: HashSet::new(),
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
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

impl DistinctStateFunc for AggregateDistinctStringState {
    fn new() -> Self {
        AggregateDistinctStringState {
            set: UnsizedHashSet::<[u8]>::with_capacity(4),
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for k in self.set.iter() {
            writer.write_binary(k.key())?;
        }
        Ok(())
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        let size = reader.read_uvarint()?;
        self.set = UnsizedHashSet::<[u8]>::with_capacity(size as usize);
        for _ in 0..size {
            let s = reader.read_uvarint()? as usize;
            let _ = self.set.set_insert(&reader[..s]);
            *reader = &reader[s..];
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
        let _ = self.set.set_insert(data);
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
                        let _ = self.set.set_insert(data);
                    }
                }
            }
            _ => {
                for row in 0..input_rows {
                    let data = column.get_data(row);
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

    // After build_columns, the set is not avaiable any more.
    fn build_columns(&mut self, _fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        let mut builder = MutableStringColumn::with_capacity(self.set.len());
        for key in self.set.iter() {
            builder.push(key.key())
        }
        Ok(vec![builder.finish().arc()])
    }
}

impl<T, E> DistinctStateFunc for AggregateDistinctPrimitiveState<T, E>
where
    T: PrimitiveType + From<E>,
    E: From<T>
        + Sync
        + Send
        + Clone
        + std::fmt::Debug
        + HashtableKeyable
        + FastHash
        + Serialize
        + DeserializeOwned,
{
    fn new() -> Self {
        AggregateDistinctPrimitiveState {
            set: CommonHashSet::with_capacity(4),
            _t: PhantomData,
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for value in self.set.iter() {
            serialize_into_buf(writer, value.key())?
        }
        Ok(())
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        let size = reader.read_uvarint()?;
        self.set = CommonHashSet::with_capacity(size as usize);
        for _ in 0..size {
            let t: E = deserialize_from_slice(reader)?;
            let _ = self.set.set_insert(t);
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.set.len() == 0
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
        self.set.set_merge(&rhs.set);
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

impl DistinctStateFunc for AggregateUniqStringState {
    fn new() -> Self {
        AggregateUniqStringState {
            set: StackHashSet::new(),
            inserted: false,
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
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

// by default it's not enabled now
#[allow(dead_code)]
pub struct AggregateDistinctTwoLevelPrimitiveState<T: PrimitiveType, E: From<T> + HashtableKeyable>
{
    set: TwoLevelHashSet<E>,
    _t: PhantomData<T>,
}

const BUCKETS: usize = 256;
impl<T, E> DistinctStateFunc for AggregateDistinctTwoLevelPrimitiveState<T, E>
where
    T: PrimitiveType + From<E>,
    E: From<T>
        + Sync
        + Send
        + Clone
        + std::fmt::Debug
        + HashtableKeyable
        + FastHash
        + Serialize
        + DeserializeOwned
        + 'static,
{
    fn new() -> Self {
        let sets: Vec<CommonHashSet<E>> = (0..BUCKETS)
            .map(|_| CommonHashSet::<E>::with_capacity(4))
            .collect();
        Self {
            set: TwoLevelHashSet::create(sets),
            _t: PhantomData,
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        let inners = self.set.inner_sets();

        for inner in inners.iter() {
            writer.write_uvarint(inner.len() as u64)?;
            for value in inner.iter() {
                serialize_into_buf(writer, value.key())?
            }
        }
        Ok(())
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        let sets: Result<Vec<CommonHashSet<E>>> = (0..BUCKETS)
            .map(|_| {
                let size = reader.read_uvarint()? as usize;
                let mut data = CommonHashSet::<E>::with_capacity(size);
                for _i in 0..size {
                    let t: E = deserialize_from_slice(reader)?;
                    let _ = data.set_insert(t);
                }
                Ok(data)
            })
            .collect();

        self.set = TwoLevelHashSet::create(sets?);
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.set.len() == 0
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: &[ColumnRef], row: usize) -> Result<()> {
        let array: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        let v = unsafe { array.value_unchecked(row) };
        self.set.set_insert(&E::from(v));
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
                        self.set.set_insert(&E::from(*t));
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let v = unsafe { array.value_unchecked(row) };
                    self.set.set_insert(&E::from(v));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    fn build_columns(&mut self, _fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        let values: Vec<T> = self.set.iter().map(|e| (*e.key()).into()).collect();
        let result = PrimitiveColumn::<T>::new_from_vec(values);
        Ok(vec![result.arc()])
    }

    fn support_merge_parallel() -> bool {
        true
    }

    fn merge_parallel(&mut self, pool: &mut ThreadPool, rhs: &mut Self) -> Result<()> {
        let mut join_handles = Vec::with_capacity(self.set.inner_sets().len());

        let left: Vec<CommonHashSet<E>> = std::mem::take(self.set.inner_sets_mut());
        let right: Vec<CommonHashSet<E>> = std::mem::take(rhs.set.inner_sets_mut());

        for (mut a, b) in left.into_iter().zip(right.into_iter()) {
            join_handles.push(pool.execute(move || {
                if a.is_empty() {
                    return b;
                }
                a.set_merge(&b);
                a
            }));
        }

        let mut vs = Vec::with_capacity(BUCKETS);
        for join_handle in join_handles {
            vs.push(join_handle.join());
        }
        *self.set.inner_sets_mut() = vs;
        Ok(())
    }
}
