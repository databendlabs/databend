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

use std::marker::PhantomData;

use databend_common_exception::Result;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::HashMethodFixedKeys;
use databend_common_hashtable::DictionaryKeys;

use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

pub trait GroupColumnsBuilder {
    type T;
    fn append_value(&mut self, v: Self::T);
    fn finish(self) -> Result<Vec<Column>>;
}

pub struct FixedKeysGroupColumnsBuilder<'a, T> {
    _t: PhantomData<&'a ()>,
    data: Vec<T>,
    group_column_indices: Vec<usize>,
    group_data_types: Vec<DataType>,
}

impl<'a, T> FixedKeysGroupColumnsBuilder<'a, T> {
    pub fn create(capacity: usize, params: &AggregatorParams) -> Self {
        Self {
            _t: Default::default(),
            data: Vec::with_capacity(capacity),
            group_column_indices: params.group_columns.clone(),
            group_data_types: params.group_data_types.clone(),
        }
    }
}

impl<'a, T: Copy + Send + Sync + 'static> GroupColumnsBuilder
    for FixedKeysGroupColumnsBuilder<'a, T>
{
    type T = &'a T;

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        self.data.push(*v);
    }

    #[inline]
    fn finish(self) -> Result<Vec<Column>> {
        let method = HashMethodFixedKeys::<T>::default();
        method.deserialize_group_columns(
            self.data,
            &self
                .group_column_indices
                .iter()
                .cloned()
                .zip(self.group_data_types.iter().cloned())
                .collect::<Vec<_>>(),
        )
    }
}

pub struct SerializedKeysGroupColumnsBuilder<'a> {
    data: Vec<&'a [u8]>,
    group_data_types: Vec<DataType>,

    single_builder: Option<BinaryColumnBuilder>,
}

impl<'a> SerializedKeysGroupColumnsBuilder<'a> {
    pub fn create(capacity: usize, data_capacity: usize, params: &AggregatorParams) -> Self {
        let (single_builder, data) = if params.group_data_types.len() == 1
            && (params.group_data_types[0].is_string() || params.group_data_types[0].is_variant())
        {
            (
                Some(BinaryColumnBuilder::with_capacity(capacity, data_capacity)),
                vec![],
            )
        } else {
            (None, Vec::with_capacity(capacity))
        };

        Self {
            data,
            group_data_types: params.group_data_types.clone(),
            single_builder,
        }
    }
}

impl<'a> GroupColumnsBuilder for SerializedKeysGroupColumnsBuilder<'a> {
    type T = &'a [u8];

    fn append_value(&mut self, v: &'a [u8]) {
        match self.single_builder.as_mut() {
            Some(builder) => {
                builder.put_slice(v);
                builder.commit_row();
            }
            None => self.data.push(v),
        }
    }

    fn finish(mut self) -> Result<Vec<Column>> {
        if let Some(builder) = self.single_builder.take() {
            let col = builder.build();
            match self.group_data_types[0] {
                DataType::String => {
                    return Ok(vec![Column::String(unsafe {
                        StringColumn::from_binary_unchecked(col)
                    })]);
                }
                DataType::Variant => return Ok(vec![Column::Variant(col)]),
                _ => {}
            }
        }

        let rows = self.data.len();
        let keys = self.data.as_mut_slice();

        let mut res = Vec::with_capacity(self.group_data_types.len());
        for data_type in self.group_data_types.iter() {
            let mut column = ColumnBuilder::with_capacity(data_type, rows);

            for (_, key) in keys.iter_mut().enumerate() {
                column.push_binary(key)?;
            }
            res.push(column.build());
        }

        Ok(res)
    }
}

pub struct DictionarySerializedKeysGroupColumnsBuilder<'a> {
    other_type_data: Vec<&'a [u8]>,
    string_type_data: Vec<DictionaryKeys>,
    group_data_types: Vec<DataType>,
}

impl<'a> DictionarySerializedKeysGroupColumnsBuilder<'a> {
    pub fn create(capacity: usize, _data_capacity: usize, params: &AggregatorParams) -> Self {
        Self {
            other_type_data: Vec::with_capacity(capacity),
            string_type_data: Vec::with_capacity(capacity),
            group_data_types: params.group_data_types.clone(),
        }
    }
}

impl<'a> GroupColumnsBuilder for DictionarySerializedKeysGroupColumnsBuilder<'a> {
    type T = &'a DictionaryKeys;

    fn append_value(&mut self, v: &'a DictionaryKeys) {
        unsafe {
            if let Some(last) = v.keys.as_ref().last() {
                self.other_type_data.push(last.as_ref());
            }
        }

        self.string_type_data.push(*v)
    }

    fn finish(mut self) -> Result<Vec<Column>> {
        let rows = self.string_type_data.len();
        let other_type_keys = self.other_type_data.as_mut_slice();

        let mut index = 0;
        let mut res = Vec::with_capacity(self.group_data_types.len());
        for data_type in self.group_data_types.iter() {
            if data_type.is_string() || data_type.is_variant() {
                let mut builder = BinaryColumnBuilder::with_capacity(0, 0);

                for string_type_keys in &self.string_type_data {
                    builder.put(unsafe { string_type_keys.keys.as_ref()[index].as_ref() });
                    builder.commit_row();
                }

                index += 1;
                res.push(match data_type.is_string() {
                    true => Column::String(unsafe {
                        StringColumn::from_binary_unchecked(builder.build())
                    }),
                    false => Column::Variant(builder.build()),
                });
            } else {
                let mut column = ColumnBuilder::with_capacity(data_type, rows);

                for (_, key) in other_type_keys.iter_mut().enumerate() {
                    column.push_binary(key)?;
                }

                res.push(column.build());
            }
        }

        Ok(res)
    }
}
