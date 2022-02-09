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
use crate::types::column::Either;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub struct NullableColumnData {
    pub inner: ArcColumnData,
    pub nulls: Vec<u8>,
}

impl NullableColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let mut nulls = vec![0; size];
        reader.read_bytes(nulls.as_mut())?;
        let inner =
            <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, type_name, size, tz)?;
        Ok(NullableColumnData { inner, nulls })
    }
}

impl ColumnData for NullableColumnData {
    fn sql_type(&self) -> SqlType {
        let inner_type = self.inner.sql_type();
        SqlType::Nullable(inner_type.into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let nulls: &[u8] = self.nulls.as_ref();
        encoder.write_bytes(&nulls[start..end]);
        self.inner.save(encoder, start, end);
    }

    fn len(&self) -> usize {
        assert_eq!(self.nulls.len(), self.inner.len());
        self.inner.len()
    }

    fn push(&mut self, value: Value) {
        let inner_column: &mut dyn ColumnData = Arc::get_mut(&mut self.inner).unwrap();

        if let Value::Nullable(e) = value {
            match e {
                Either::Left(sql_type) => {
                    let default_value = Value::default(sql_type.clone());
                    inner_column.push(default_value);
                    self.nulls.push(true as u8);
                }
                Either::Right(inner) => {
                    inner_column.push(*inner);
                    self.nulls.push(false as u8);
                }
            }
        } else {
            inner_column.push(value);
            self.nulls.push(false as u8);
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        if self.nulls[index] == 1 {
            let sql_type = self.inner.sql_type();
            ValueRef::Nullable(Either::Left(sql_type.into()))
        } else {
            let inner_value = self.inner.at(index);
            ValueRef::Nullable(Either::Right(Box::new(inner_value)))
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone(),
            nulls: self.nulls.clone(),
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        if level == self.sql_type().level() {
            *pointers[0] = self.nulls.as_ptr();
            *(pointers[1] as *mut usize) = self.len();
            Ok(())
        } else {
            self.inner.get_internal(pointers, level)
        }
    }

    fn cast_to(&self, _this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        if let SqlType::Nullable(inner_target) = target {
            if let Some(inner) = self.inner.cast_to(&self.inner, inner_target) {
                return Some(Arc::new(NullableColumnData {
                    inner,
                    nulls: self.nulls.clone(),
                }));
            }
        }
        None
    }
}
