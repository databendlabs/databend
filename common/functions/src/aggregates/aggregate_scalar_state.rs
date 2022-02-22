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

use std::cmp::Ordering;
use std::marker::PhantomData;

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

pub trait ScalarStateFunc<S: Scalar>: Send + Sync + 'static {
    fn new() -> Self;
    fn add(&mut self, value: S::RefType<'_>);
    fn add_batch(&mut self, column: &ColumnRef, validity: Option<&Bitmap>) -> Result<()>;

    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn serialize(&self, writer: &mut BytesMut) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn merge_result(&mut self, column: &mut dyn MutableColumn) -> Result<()>;
}

pub trait ChangeIf<S: Scalar>: Send + Sync + 'static {
    fn change_if(l: S::RefType<'_>, r: S::RefType<'_>) -> bool;
}

#[derive(Default)]
pub struct CmpMin {}

impl<S> ChangeIf<S> for CmpMin
where
    S: Scalar,
    for<'a> S::RefType<'a>: PartialOrd,
{
    #[inline]
    fn change_if<'a>(l: S::RefType<'_>, r: S::RefType<'_>) -> bool {
        let l = S::upcast_gat(l);
        let r = S::upcast_gat(r);
        l.partial_cmp(&r).unwrap_or(Ordering::Equal) == Ordering::Greater
    }
}

#[derive(Default)]
pub struct CmpMax {}

impl<S> ChangeIf<S> for CmpMax
where
    S: Scalar,
    for<'a> S::RefType<'a>: PartialOrd,
{
    #[inline]
    fn change_if<'a>(l: S::RefType<'_>, r: S::RefType<'_>) -> bool {
        let l = S::upcast_gat(l);
        let r = S::upcast_gat(r);

        l.partial_cmp(&r).unwrap_or(Ordering::Equal) == Ordering::Less
    }
}

#[derive(Default)]
pub struct CmpAny {}

impl<S> ChangeIf<S> for CmpAny
where
    S: Scalar,
    for<'a> S::RefType<'a>: PartialOrd,
{
    #[inline]
    fn change_if<'a>(_l: S::RefType<'_>, _r: S::RefType<'_>) -> bool {
        false
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct ScalarState<S: Scalar, C> {
    #[serde(bound(deserialize = "S: DeserializeOwned"))]
    pub value: Option<S>,
    #[serde(skip)]
    _c: PhantomData<C>,
}

impl<S, C> ScalarStateFunc<S> for ScalarState<S, C>
where
    S: Scalar + Send + Sync + Serialize + DeserializeOwned,
    C: ChangeIf<S> + Default,
{
    fn new() -> Self {
        Self::default()
    }
    fn add(&mut self, other: S::RefType<'_>) {
        match &self.value {
            Some(a) => {
                if C::change_if(a.as_scalar_ref(), other) {
                    self.value = Some(other.to_owned_scalar());
                }
            }
            _ => self.value = Some(other.to_owned_scalar()),
        }
    }

    fn add_batch(&mut self, column: &ColumnRef, validity: Option<&Bitmap>) -> Result<()> {
        let col: &<S as Scalar>::ColumnType = unsafe { Series::static_cast(column) };
        if let Some(bit) = validity {
            if bit.null_count() == column.len() {
                return Ok(());
            }
            let mut v = S::default();
            let mut has_v = false;

            let viewer = S::try_create_viewer(column)?;
            for (row, data) in viewer.iter().enumerate() {
                if viewer.null_at(row) {
                    continue;
                }
                if !has_v {
                    has_v = true;
                    v = data.to_owned_scalar();
                } else if C::change_if(v.as_scalar_ref(), data) {
                    v = data.to_owned_scalar();
                }
            }

            if has_v {
                self.add(v.as_scalar_ref());
            }
        } else {
            let v = col
                .scalar_iter()
                .reduce(|a, b| if !C::change_if(a, b) { a } else { b });

            if let Some(v) = v {
                self.add(v);
            }
        };
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(value) = &rhs.value {
            self.add(value.as_scalar_ref());
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

    fn merge_result(&mut self, column: &mut dyn MutableColumn) -> Result<()> {
        let builder: &mut <<S as Scalar>::ColumnType as ScalarColumn>::Builder =
            Series::check_get_mutable_column(column)?;

        if let Some(val) = &self.value {
            builder.push(val.as_scalar_ref());
        } else {
            // TODO make it nullable or default ?
            builder.push(S::default().as_scalar_ref());
        }
        Ok(())
    }
}
