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

use std::convert;
use std::sync::Arc;

use crate::binary::Encoder;
use crate::errors::Error;
use crate::errors::FromSqlError;
use crate::errors::Result;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub type ArcColumnData = Arc<dyn ColumnData + Send + Sync>;

pub type BoxColumnData = Box<dyn ColumnData + Send + Sync>;

pub trait ColumnData {
    fn sql_type(&self) -> SqlType;
    fn save(&self, encoder: &mut Encoder, start: usize, end: usize);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn push(&mut self, value: Value);
    fn at(&self, index: usize) -> ValueRef;

    fn clone_instance(&self) -> BoxColumnData;

    #[allow(clippy::missing_safety_doc)]
    unsafe fn get_internal(&self, _pointers: &[*mut *const u8], _level: u8) -> Result<()> {
        Err(Error::FromSql(FromSqlError::UnsupportedOperation))
    }

    fn cast_to(&self, _this: &ArcColumnData, _target: &SqlType) -> Option<ArcColumnData> {
        None
    }
}

pub trait ColumnDataExt {
    fn append<T: convert::Into<Value>>(&mut self, value: T);
}

impl<C: ColumnData> ColumnDataExt for C {
    fn append<T: convert::Into<Value>>(&mut self, value: T) {
        self.push(value.into());
    }
}
