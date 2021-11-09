// Copyright 2020 Datafuse Labs.
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

use std::borrow::Cow;
use std::marker;

use chrono_tz::Tz;

use crate::errors::Error;
use crate::errors::FromSqlError;
use crate::errors::Result;
use crate::types::block::ColumnIdx;
use crate::types::column::ArcColumnWrapper;
use crate::types::column::ColumnData;
use crate::types::column::Either;
use crate::types::Column;
use crate::types::ColumnType;
use crate::types::SqlType;
use crate::types::Value;
use crate::Block;

pub trait RowBuilder {
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()>;
}

pub struct RNil;

pub struct RCons<T>
where T: RowBuilder
{
    key: Cow<'static, str>,
    value: Value,
    tail: T,
}

impl RNil {
    pub fn put(self, key: Cow<'static, str>, value: Value) -> RCons<Self> {
        RCons {
            key,
            value,
            tail: RNil,
        }
    }
}

impl<T> RCons<T>
where T: RowBuilder
{
    pub fn put(self, key: Cow<'static, str>, value: Value) -> RCons<Self> {
        RCons {
            key,
            value,
            tail: self,
        }
    }
}

impl RowBuilder for RNil {
    #[inline(always)]
    fn apply<K: ColumnType>(self, _block: &mut Block<K>) -> Result<()> {
        Ok(())
    }
}

impl<T> RowBuilder for RCons<T>
where T: RowBuilder
{
    #[inline(always)]
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()> {
        put_param(self.key, self.value, block)?;
        self.tail.apply(block)
    }
}

impl RowBuilder for Vec<(String, Value)> {
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()> {
        for (k, v) in self {
            put_param(k.into(), v, block)?;
        }
        Ok(())
    }
}

fn put_param<K: ColumnType>(
    key: Cow<'static, str>,
    value: Value,
    block: &mut Block<K>,
) -> Result<()> {
    let col_index = match key.as_ref().get_index(&block.columns) {
        Ok(col_index) => col_index,
        Err(Error::FromSql(FromSqlError::OutOfRange)) => {
            if block.row_count() <= 1 {
                let sql_type: SqlType = From::from(value.clone());

                let timezone = extract_timezone(&value);

                let column = Column {
                    name: key.clone().into(),
                    data: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                        sql_type,
                        timezone,
                        block.capacity,
                    )?,
                    _marker: marker::PhantomData,
                };

                block.columns.push(column);
                return put_param(key, value, block);
            } else {
                return Err(Error::FromSql(FromSqlError::OutOfRange));
            }
        }
        Err(err) => return Err(err),
    };

    block.columns[col_index].push(value);
    Ok(())
}

fn extract_timezone(value: &Value) -> Tz {
    match value {
        Value::Date(_, tz) => *tz,
        Value::DateTime(_, tz) => *tz,
        Value::Nullable(Either::Right(d)) => extract_timezone(d),
        Value::Array(_, data) => {
            if let Some(v) = data.first() {
                extract_timezone(v)
            } else {
                Tz::Zulu
            }
        }
        _ => Tz::Zulu,
    }
}
