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
use common_io::prelude::*;
use serde::de::DeserializeOwned;
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
    fn build_columns(&self, fields: &[DataField]) -> Result<Vec<ColumnRef>>;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct DataGroupValues(Vec<DataGroupValue>);

pub struct AggregateDistinctState {
    set: HashSet<DataGroupValues, RandomState>,
}

pub struct AggregateDistinctPrimitiveState<T: PrimitiveType, E: From<T>> {
    set: HashSet<E, RandomState>,
    _t: PhantomData<T>,
}

pub struct AggregateDistinctStringState {
    set: HashSet<Vec<u8>, RandomState>,
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

    fn build_columns(&self, fields: &[DataField]) -> Result<Vec<ColumnRef>> {
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

impl DistinctStateFunc<Vec<u8>> for AggregateDistinctStringState {
    fn new() -> Self {
        AggregateDistinctStringState {
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
        let value = columns.get(0).map(|s| s.get(row)).unwrap();
        self.set.insert(value.as_string().unwrap());
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        for row in 0..input_rows {
            let value = columns.get(0).map(|s| s.get(row)).unwrap();
            match validity {
                Some(v) => {
                    if v.get_bit(row) {
                        self.set.insert(value.as_string().unwrap());
                    }
                }
                None => {
                    self.set.insert(value.as_string().unwrap());
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.extend(rhs.set.clone());
        Ok(())
    }

    fn build_columns(&self, _fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        let mut result = Vec::with_capacity(self.set.len());
        self.set.iter().for_each(|v| {
            result.push(v.as_slice());
        });
        let columns = StringColumn::from_iterator(result.into_iter());
        Ok(vec![columns.arc()])
    }
}

impl<T, E> DistinctStateFunc<T> for AggregateDistinctPrimitiveState<T, E>
where
    T: PrimitiveType,
    E: From<T> + Into<T> + Hash + Eq + Sync + Send + Serialize + Clone + DeserializeOwned,
{
    fn new() -> Self {
        AggregateDistinctPrimitiveState {
            set: HashSet::new(),
            _t: PhantomData,
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
        let array: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        let v = unsafe { array.value_unchecked(row) };
        self.set.insert(E::from(v));
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        for row in 0..input_rows {
            let array: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
            let value = unsafe { array.value_unchecked(row) };
            match validity {
                Some(v) => {
                    if v.get_bit(row) {
                        self.set.insert(E::from(value));
                    }
                }
                None => {
                    self.set.insert(E::from(value));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.extend(rhs.set.clone());
        Ok(())
    }

    fn build_columns(&self, fields: &[DataField]) -> Result<Vec<ColumnRef>> {
        let mut results: Vec<T> = Vec::with_capacity(self.set.len());
        self.set.iter().for_each(|v| {
            results.push(v.clone().into());
        });
        let data_type = fields[0].data_type();
        let columns = results
            .iter()
            .map(|v| DataValue::try_from(*v).unwrap())
            .collect::<Vec<DataValue>>();
        let array = columns.as_slice();
        let column = data_type.create_column(array).unwrap();
        Ok((&[column]).to_vec())
    }
}
