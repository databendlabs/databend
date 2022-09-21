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

use std::cmp::Ordering;
use std::marker::PhantomData;

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_io::prelude::deserialize_from_slice;
use common_io::prelude::serialize_into_buf;
use serde::Deserialize;
use serde::Serialize;

pub trait ChangeIf: Send + Sync + 'static {
    fn change_if(l: ScalarRef<'_>, r: ScalarRef<'_>) -> bool;
}

#[derive(Default)]
pub struct CmpMin;

impl ChangeIf for CmpMin {
    #[inline]
    fn change_if(l: ScalarRef<'_>, r: ScalarRef<'_>) -> bool {
        l.partial_cmp(&r).unwrap_or(Ordering::Equal) == Ordering::Greater
    }
}

#[derive(Default)]
pub struct CmpMax;

impl ChangeIf for CmpMax {
    #[inline]
    fn change_if(l: ScalarRef<'_>, r: ScalarRef<'_>) -> bool {
        l.partial_cmp(&r).unwrap_or(Ordering::Equal) == std::cmp::Ordering::Less
    }
}

#[derive(Default)]
pub struct CmpAny;

impl ChangeIf for CmpAny {
    #[inline]
    fn change_if(_: ScalarRef<'_>, _: ScalarRef<'_>) -> bool {
        false
    }
}

pub trait ScalarStateFunc: Send + Sync + 'static {
    fn new() -> Self;
    fn add(&mut self, other: ScalarRef<'_>);
    fn add_batch(&mut self, column: &Column, validity: Option<&Bitmap>) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()>;
    fn serialize(&self, writer: &mut BytesMut) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
}

#[derive(Default, Serialize, Deserialize)]
pub struct ScalarState<C> {
    pub value: Option<Scalar>,
    #[serde(skip)]
    _c: PhantomData<C>,
}

impl<C> ScalarStateFunc for ScalarState<C>
where C: ChangeIf + Default
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: ScalarRef<'_>) {
        match &self.value {
            Some(v) => {
                if C::change_if(v.as_ref(), other.clone()) {
                    self.value = Some(other.to_owned());
                }
            }
            None => {
                self.value = Some(other.to_owned());
            }
        }
    }

    fn add_batch(&mut self, column: &Column, validity: Option<&Bitmap>) -> Result<()> {
        if column.len() == 0 {
            return Ok(());
        }

        if let Some(validity) = validity {
            if validity.unset_bits() == column.len() {
                return Ok(());
            }

            let mut v = column.index(0).unwrap();
            let mut has_v = validity.get_bit(0);

            for (data, valid) in column.iter().skip(1).zip(validity.iter().skip(1)) {
                if !valid {
                    continue;
                }
                if !has_v {
                    has_v = true;
                    v = data.clone();
                } else if C::change_if(v.clone(), data.clone()) {
                    v = data.clone();
                }
            }

            if has_v {
                self.add(v);
            }
        } else {
            let v = column.iter().reduce(|l, r| {
                if !C::change_if(l.clone(), r.clone()) {
                    l
                } else {
                    r
                }
            });
            if let Some(v) = v {
                self.add(v);
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(v) = &rhs.value {
            self.add(v.as_ref());
        }
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        if let Some(v) = &self.value {
            builder.push(v.as_ref());
        } else {
            builder.push_default();
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, self)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = deserialize_from_slice(reader)?;
        Ok(())
    }
}

pub fn need_manual_drop_state(data_type: &DataType) -> bool {
    match data_type {
        DataType::String | DataType::Variant => true,
        DataType::Nullable(t) | DataType::Array(t) | DataType::Map(t) => need_manual_drop_state(t),
        DataType::Tuple(ts) => ts.iter().any(need_manual_drop_state),
        _ => false,
    }
}
