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

use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;

use super::column_data::ColumnData;
use super::ColumnFrom;
use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Result;
use crate::types::column::column_data::BoxColumnData;
use crate::types::column::nullable::NullableColumnData;
use crate::types::column::ColumnWrapper;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub(crate) trait IpVersion: Copy + Sync + Send + 'static {
    fn sql_type() -> SqlType;
    fn size() -> usize;
    fn push(inner: &mut Vec<u8>, value: Value);
    fn get(inner: &[u8], index: usize) -> ValueRef;
}

#[derive(Copy, Clone)]
pub(crate) struct Ipv4;

#[derive(Copy, Clone)]
pub(crate) struct Ipv6;

#[derive(Copy, Clone)]
pub(crate) struct Uuid;

impl IpVersion for Ipv4 {
    #[inline(always)]
    fn sql_type() -> SqlType {
        SqlType::Ipv4
    }

    #[inline(always)]
    fn size() -> usize {
        4
    }

    #[inline(always)]
    fn push(inner: &mut Vec<u8>, value: Value) {
        if let Value::Ipv4(v) = value {
            inner.extend(&v);
        } else {
            panic!();
        }
    }

    #[inline(always)]
    fn get(inner: &[u8], index: usize) -> ValueRef {
        let mut v: [u8; 4] = Default::default();
        v.copy_from_slice(&inner[index * 4..(index + 1) * 4]);
        ValueRef::Ipv4(v)
    }
}

impl IpVersion for Ipv6 {
    #[inline(always)]
    fn sql_type() -> SqlType {
        SqlType::Ipv6
    }

    #[inline(always)]
    fn size() -> usize {
        16
    }

    #[inline(always)]
    fn push(inner: &mut Vec<u8>, value: Value) {
        if let Value::Ipv6(v) = value {
            inner.extend(&v);
        } else {
            panic!();
        }
    }

    #[inline(always)]
    fn get(inner: &[u8], index: usize) -> ValueRef {
        let mut v: [u8; 16] = Default::default();
        v.copy_from_slice(&inner[index * 16..(index + 1) * 16]);
        ValueRef::Ipv6(v)
    }
}

impl IpVersion for Uuid {
    #[inline(always)]
    fn sql_type() -> SqlType {
        SqlType::Uuid
    }

    #[inline(always)]
    fn size() -> usize {
        16
    }

    #[inline(always)]
    fn push(inner: &mut Vec<u8>, value: Value) {
        if let Value::Uuid(v) = value {
            inner.extend(&v);
        } else {
            panic!();
        }
    }

    #[inline(always)]
    fn get(inner: &[u8], index: usize) -> ValueRef {
        let mut v: [u8; 16] = Default::default();
        v.copy_from_slice(&inner[index * 16..(index + 1) * 16]);
        ValueRef::Uuid(v)
    }
}

impl ColumnFrom for Vec<Ipv4Addr> {
    fn column_from<W: ColumnWrapper>(data: Self) -> W::Wrapper {
        let mut inner = Vec::with_capacity(data.len());
        for ip in data {
            let mut buffer = ip.octets();
            buffer.reverse();
            inner.extend(&buffer);
        }

        W::wrap(IpColumnData::<Ipv4> {
            inner,
            phantom: PhantomData,
        })
    }
}

impl ColumnFrom for Vec<Ipv6Addr> {
    fn column_from<W: ColumnWrapper>(data: Self) -> W::Wrapper {
        let mut inner = Vec::with_capacity(data.len());
        for ip in data {
            inner.extend(&ip.octets());
        }

        W::wrap(IpColumnData::<Ipv6> {
            inner,
            phantom: PhantomData,
        })
    }
}

impl ColumnFrom for Vec<uuid::Uuid> {
    fn column_from<W: ColumnWrapper>(data: Self) -> W::Wrapper {
        let mut inner = Vec::with_capacity(data.len());
        for uuid in data {
            let mut buffer = *uuid.as_bytes();
            buffer[..8].reverse();
            buffer[8..].reverse();
            inner.extend(&buffer);
        }

        W::wrap(IpColumnData::<Uuid> {
            inner,
            phantom: PhantomData,
        })
    }
}

impl ColumnFrom for Vec<Option<Ipv4Addr>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let n = source.len();
        let mut inner: Vec<u8> = Vec::with_capacity(n * 4);
        let mut nulls: Vec<u8> = Vec::with_capacity(n);

        for ip in source {
            match ip {
                None => {
                    inner.extend(&[0; 4]);
                    nulls.push(1);
                }
                Some(ip) => {
                    let mut buffer = ip.octets();
                    buffer.reverse();
                    inner.extend(&buffer);
                    nulls.push(0);
                }
            }
        }

        let inner = Arc::new(IpColumnData::<Ipv4> {
            inner,
            phantom: PhantomData,
        });

        let data = NullableColumnData { inner, nulls };

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Option<Ipv6Addr>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let n = source.len();
        let mut inner: Vec<u8> = Vec::with_capacity(n * 16);
        let mut nulls: Vec<u8> = Vec::with_capacity(n);

        for ip in source {
            match ip {
                None => {
                    inner.extend(&[0; 16]);
                    nulls.push(1);
                }
                Some(ip) => {
                    inner.extend(&ip.octets());
                    nulls.push(0);
                }
            }
        }

        let inner = Arc::new(IpColumnData::<Ipv6> {
            inner,
            phantom: PhantomData,
        });

        let data = NullableColumnData { inner, nulls };

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Option<uuid::Uuid>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let n = source.len();
        let mut inner: Vec<u8> = Vec::with_capacity(n * 16);
        let mut nulls: Vec<u8> = Vec::with_capacity(n);

        for uuid in source {
            match uuid {
                None => {
                    inner.extend(&[0; 16]);
                    nulls.push(1);
                }
                Some(uuid) => {
                    let mut buffer = *uuid.as_bytes();
                    buffer[..8].reverse();
                    buffer[8..].reverse();
                    inner.extend(&buffer);
                    nulls.push(0);
                }
            }
        }

        let inner = Arc::new(IpColumnData::<Uuid> {
            inner,
            phantom: PhantomData,
        });

        let data = NullableColumnData { inner, nulls };

        W::wrap(data)
    }
}

pub(crate) struct IpColumnData<V: IpVersion> {
    pub(crate) inner: Vec<u8>,
    pub(crate) phantom: PhantomData<V>,
}

impl<V: IpVersion> IpColumnData<V> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: vec![0; capacity * V::size()],
            phantom: PhantomData,
        }
    }

    pub(crate) fn load<R: ReadEx>(reader: &mut R, size: usize) -> Result<Self> {
        let mut inner = vec![0; size * V::size()];
        reader.read_bytes(inner.as_mut())?;

        Ok(Self {
            inner,
            phantom: PhantomData,
        })
    }
}

impl<V: IpVersion> ColumnData for IpColumnData<V> {
    fn sql_type(&self) -> SqlType {
        V::sql_type()
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let start_index = start * V::size();
        let end_index = end * V::size();

        let slice: &[u8] = self.inner.as_ref();
        encoder.write_bytes(&slice[start_index..end_index]);
    }

    fn len(&self) -> usize {
        self.inner.len() / V::size()
    }

    fn push(&mut self, value: Value) {
        V::push(&mut self.inner, value)
    }

    fn at(&self, index: usize) -> ValueRef {
        V::get(&self.inner, index)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone(),
            phantom: PhantomData,
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        assert_eq!(level, 0);
        *pointers[0] = &self.inner as *const Vec<u8> as *const u8;
        Ok(())
    }
}
