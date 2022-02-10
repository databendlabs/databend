// Copyright 2021 Datafuse Labs.
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

use chrono_tz::Tz;

use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Result;
use crate::types::column::column_data::ArcColumnData;
use crate::types::column::column_data::BoxColumnData;
use crate::types::column::ArcColumnWrapper;
use crate::types::column::ColumnData;
use crate::types::column::ColumnFrom;
use crate::types::column::ColumnWrapper;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub struct TupleColumnData {
    pub inner: Vec<ArcColumnData>,
}

impl TupleColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        inner_types: Vec<&str>,
        rows: usize,
        tz: Tz,
    ) -> Result<Self> {
        let inner = inner_types
            .into_iter()
            .map(|type_name| {
                <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, type_name, rows, tz)
                    .unwrap()
            })
            .collect();
        Ok(Self { inner })
    }
}

impl ColumnFrom for Vec<ArcColumnData> {
    fn column_from<W: ColumnWrapper>(inner: Self) -> W::Wrapper {
        W::wrap(TupleColumnData { inner })
    }
}

impl ColumnData for TupleColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Tuple(self.inner.iter().map(|c| c.sql_type().into()).collect())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        self.inner.iter().for_each(|c| c.save(encoder, start, end));
    }

    fn len(&self) -> usize {
        self.inner[0].len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Tuple(vs) = value {
            vs.iter().zip(self.inner.iter_mut()).for_each(|(v, c)| {
                let inner_column = Arc::get_mut(c).unwrap();
                inner_column.push(v.clone());
            });
        } else {
            panic!("value should be a tuple");
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        ValueRef::Tuple(Arc::new(self.inner.iter().map(|c| c.at(index)).collect()))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone(),
        })
    }

    fn cast_to(&self, _this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        if let SqlType::Tuple(target_types) = target {
            if target_types.len() == self.inner.len() {
                let mut new_inner = Vec::with_capacity(self.inner.len());
                for (i, c) in self.inner.iter().enumerate() {
                    let target_type = target_types[i];
                    let new_c = c.cast_to(c, target_type);
                    if let Some(new_c) = new_c {
                        new_inner.push(new_c);
                    } else {
                        return None;
                    }
                }
                return Some(Arc::new(TupleColumnData { inner: new_inner }));
            }
        }
        None
    }
}
