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

use std::collections::HashSet;
use std::hash::Hasher;
use std::marker::Send;
use std::sync::Arc;

use borsh::BorshSerialize;
use bumpalo::Bump;
use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::*;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;
use databend_common_hashtable::HashSet as CommonHashSet;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::HashtableLike;
use databend_common_hashtable::ShortStringHashSet;
use databend_common_hashtable::StackHashSet;
use databend_common_io::prelude::*;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use super::batch_merge1;
use super::batch_serialize1;
use super::borsh_partial_deserialize;
use super::SerializeInfo;
use super::StateAddr;
use super::StateSerde;

pub(super) trait DistinctStateFunc: Sized + Send + StateSerde + 'static {
    fn new() -> Self;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn add(&mut self, columns: ProjectedBlock, row: usize, check_null: bool) -> Result<()>;
    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
        check_null: bool,
    ) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn build_entries(&mut self, types: &[DataType]) -> Result<Vec<BlockEntry>>;
}

pub struct AggregateDistinctState {
    set: HashSet<Vec<u8>>,
}

impl DistinctStateFunc for AggregateDistinctState {
    fn new() -> Self {
        AggregateDistinctState {
            set: HashSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: ProjectedBlock, row: usize, check_null: bool) -> Result<()> {
        let values = columns
            .iter()
            .map(|entry| unsafe { entry.index_unchecked(row) }.to_owned())
            .collect::<Vec<_>>();

        if check_null && values.iter().all(Scalar::is_null) {
            return Ok(());
        }

        let mut buffer = Vec::with_capacity(values.len() * std::mem::size_of::<Scalar>());
        values.serialize(&mut buffer)?;
        self.set.insert(buffer);
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
        check_null: bool,
    ) -> Result<()> {
        match validity {
            Some(validity) => {
                for (row, b) in (0..input_rows).zip(validity) {
                    if !b {
                        continue;
                    }
                    self.add(columns, row, check_null)?;
                }
            }
            None => {
                for row in 0..input_rows {
                    self.add(columns, row, check_null)?;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.extend(rhs.set.clone());
        Ok(())
    }

    fn build_entries(&mut self, types: &[DataType]) -> Result<Vec<BlockEntry>> {
        let mut builders: Vec<ColumnBuilder> = types
            .iter()
            .map(|ty| ColumnBuilder::with_capacity(ty, self.set.len()))
            .collect();

        for data in self.set.iter() {
            let mut slice = data.as_slice();
            let scalars: Vec<Scalar> = borsh_partial_deserialize(&mut slice)?;
            scalars.iter().enumerate().for_each(|(idx, group_value)| {
                builders[idx].push(group_value.as_ref());
            });
        }

        Ok(builders.into_iter().map(|b| b.build().into()).collect())
    }
}

impl StateSerde for AggregateDistinctState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(DataType::Binary)).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<BinaryType>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for v in state.set.iter() {
                    builder.put_item(v);
                }
                builder.commit_row();
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<BinaryType>, Self, _>(places, loc, state, filter, |state, data| {
            for v in data.iter() {
                state.set.insert(v.to_vec());
            }
            Ok(())
        })
    }
}

pub struct AggregateDistinctStringState {
    set: ShortStringHashSet<[u8]>,
}

impl DistinctStateFunc for AggregateDistinctStringState {
    fn new() -> Self {
        #![allow(clippy::arc_with_non_send_sync)]
        AggregateDistinctStringState {
            set: ShortStringHashSet::<[u8]>::with_capacity(4, Arc::new(Bump::new())),
        }
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: ProjectedBlock, row: usize, _check_null: bool) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        let data = unsafe { view.index_unchecked(row) };
        let _ = self.set.set_insert(data.as_bytes());
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
        _check_null: bool,
    ) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        match validity {
            Some(v) => {
                for row in 0..input_rows {
                    if v.get_bit(row) {
                        let data = unsafe { view.index_unchecked(row) };
                        let _ = self.set.set_insert(data.as_bytes());
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let data = unsafe { view.index_unchecked(row) };
                    let _ = self.set.set_insert(data.as_bytes());
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    fn build_entries(&mut self, _types: &[DataType]) -> Result<Vec<BlockEntry>> {
        let mut builder = StringColumnBuilder::with_capacity(self.set.len());
        for key in self.set.iter() {
            builder.put_and_commit(unsafe { std::str::from_utf8_unchecked(key.key()) });
        }
        Ok(vec![Column::String(builder.build()).into()])
    }
}

impl StateSerde for AggregateDistinctStringState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(DataType::Binary)).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<BinaryType>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for v in state.set.iter() {
                    builder.put_item(v.key());
                }
                builder.commit_row();
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<BinaryType>, Self, _>(places, loc, state, filter, |state, data| {
            for v in data.iter() {
                let _ = state.set.set_insert(v);
            }
            Ok(())
        })
    }
}

pub struct AggregateDistinctNumberState<T: Number + HashtableKeyable> {
    set: CommonHashSet<T>,
}

impl<T> DistinctStateFunc for AggregateDistinctNumberState<T>
where T: Number + HashtableKeyable
{
    fn new() -> Self {
        AggregateDistinctNumberState {
            set: CommonHashSet::with_capacity(4),
        }
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: ProjectedBlock, row: usize, _check_null: bool) -> Result<()> {
        let view = columns[0].downcast::<NumberType<T>>().unwrap();
        let v = unsafe { view.index_unchecked(row) };
        let _ = self.set.set_insert(v).is_ok();
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
        _check_null: bool,
    ) -> Result<()> {
        let view = columns[0].downcast::<NumberType<T>>().unwrap();
        match validity {
            Some(bitmap) => {
                for (t, v) in view.iter().zip(bitmap.iter()) {
                    if v {
                        let _ = self.set.set_insert(t).is_ok();
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let v = unsafe { view.index_unchecked(row) };
                    let _ = self.set.set_insert(v).is_ok();
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    fn build_entries(&mut self, _types: &[DataType]) -> Result<Vec<BlockEntry>> {
        let values: Buffer<T> = self.set.iter().map(|e| *e.key()).collect();
        Ok(vec![NumberType::<T>::upcast_column(values).into()])
    }
}

impl<T> StateSerde for AggregateDistinctNumberState<T>
where T: Number + HashtableKeyable
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(NumberType::<T>::data_type())).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<NumberType<T>>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for v in state.set.iter() {
                    builder.put_item(*v.key());
                }
                builder.commit_row();
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<NumberType<T>>, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, data| {
                for v in data.iter() {
                    let _ = state.set.set_insert(*v);
                }
                Ok(())
            },
        )
    }
}

// For count(distinct string) and uniq(string)
pub struct AggregateUniqStringState {
    set: StackHashSet<u128>,
}

impl DistinctStateFunc for AggregateUniqStringState {
    fn new() -> Self {
        AggregateUniqStringState {
            set: StackHashSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn add(&mut self, columns: ProjectedBlock, row: usize, _check_null: bool) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        let data = unsafe { view.index_unchecked(row) }.as_bytes();
        let mut hasher = SipHasher24::new();
        hasher.write(data);
        let hash128 = hasher.finish128();
        let _ = self.set.set_insert(hash128.into()).is_ok();
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
        _check_null: bool,
    ) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        match validity {
            Some(v) => {
                for (t, v) in view.iter().zip(v.iter()) {
                    if v {
                        let mut hasher = SipHasher24::new();
                        hasher.write(t.as_bytes());
                        let hash128 = hasher.finish128();
                        let _ = self.set.set_insert(hash128.into()).is_ok();
                    }
                }
            }
            _ => {
                for row in 0..input_rows {
                    let data = unsafe { view.index_unchecked(row) };
                    let mut hasher = SipHasher24::new();
                    hasher.write(data.as_bytes());
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
    fn build_entries(&mut self, _types: &[DataType]) -> Result<Vec<BlockEntry>> {
        Ok(vec![])
    }
}

impl AggregateUniqStringState {
    pub fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for value in self.set.iter() {
            value.key().serialize(writer)?;
        }
        Ok(())
    }

    pub fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        let size = reader.read_uvarint()?;
        let mut set = StackHashSet::with_capacity(size as usize);
        for _ in 0..size {
            let e = borsh_partial_deserialize(reader)?;
            let _ = set.set_insert(e).is_ok();
        }
        Ok(Self { set })
    }
}

impl StateSerde for AggregateUniqStringState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state: &mut Self = AggrState::new(*place, loc).get();
            state.serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<BinaryType, Self, _>(places, loc, state, filter, |state, mut data| {
            let rhs = Self::deserialize(&mut data)?;
            state.merge(&rhs)
        })
    }
}
