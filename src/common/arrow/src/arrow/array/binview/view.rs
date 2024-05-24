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

use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Add;

use bytemuck::Pod;
use bytemuck::Zeroable;

use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::PrimitiveType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::types::NativeType;

#[derive(Debug, Copy, Clone, Default)]
#[repr(C)]
pub struct View {
    /// The length of the string/bytes.
    pub length: u32,
    /// First 4 bytes of string/bytes data.
    pub prefix: u32,
    /// The buffer index.
    pub buffer_idx: u32,
    /// The offset into the buffer.
    pub offset: u32,
}

impl View {
    #[inline(always)]
    pub fn as_u128(self) -> u128 {
        unsafe { std::mem::transmute(self) }
    }
}

impl Display for View {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl PartialEq for View {
    fn eq(&self, other: &Self) -> bool {
        self.as_u128() == other.as_u128()
    }
}
unsafe impl Pod for View {}
unsafe impl Zeroable for View {}

impl NativeType for View {
    const PRIMITIVE: PrimitiveType = PrimitiveType::UInt128;
    type Bytes = [u8; 16];

    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        self.as_u128().to_le_bytes()
    }

    #[inline]
    fn to_be_bytes(&self) -> Self::Bytes {
        self.as_u128().to_be_bytes()
    }

    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        Self::from(u128::from_le_bytes(bytes))
    }

    #[inline]
    fn from_be_bytes(bytes: Self::Bytes) -> Self {
        Self::from(u128::from_be_bytes(bytes))
    }
}

impl Add<Self> for View {
    type Output = View;

    fn add(self, _rhs: Self) -> Self::Output {
        unimplemented!()
    }
}

impl num_traits::Zero for View {
    fn zero() -> Self {
        Default::default()
    }

    fn is_zero(&self) -> bool {
        *self == Self::zero()
    }
}

impl From<u128> for View {
    #[inline]
    fn from(value: u128) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl From<View> for u128 {
    #[inline]
    fn from(value: View) -> Self {
        value.as_u128()
    }
}

fn validate_view<F>(views: &[View], buffers: &[Buffer<u8>], validate_bytes: F) -> Result<()>
where F: Fn(&[u8]) -> Result<()> {
    for view in views {
        let len = view.length;
        if len <= 12 {
            if len < 12 && view.as_u128() >> (32 + len * 8) != 0 {
                return Err(Error::oos("view contained non-zero padding in prefix"));
            }

            validate_bytes(&view.to_le_bytes()[4..4 + len as usize])?;
        } else {
            let data = buffers.get(view.buffer_idx as usize).ok_or_else(|| {
                Error::oos(format!(
                    "view index out of bounds\n\nGot: {} buffers and index: {}",
                    buffers.len(),
                    view.buffer_idx
                ))
            })?;

            let start = view.offset as usize;
            let end = start + len as usize;
            let b = data
                .as_slice()
                .get(start..end)
                .ok_or_else(|| Error::oos("buffer slice out of bounds"))?;

            if !b.starts_with(&view.prefix.to_le_bytes()) {
                return Err(Error::oos("prefix does not match string data"));
            }
            validate_bytes(b)?;
        };
    }

    Ok(())
}

pub(super) fn validate_binary_view(views: &[View], buffers: &[Buffer<u8>]) -> Result<()> {
    validate_view(views, buffers, |_| Ok(()))
}

fn validate_utf8(b: &[u8]) -> Result<()> {
    match simdutf8::basic::from_utf8(b) {
        Ok(_) => Ok(()),
        Err(_) => Err(Error::oos("invalid utf8")),
    }
}

pub(super) fn validate_utf8_view(views: &[View], buffers: &[Buffer<u8>]) -> Result<()> {
    validate_view(views, buffers, validate_utf8)
}

/// # Safety
/// The views and buffers must uphold the invariants of BinaryView otherwise we will go OOB.
pub(super) unsafe fn validate_utf8_only(views: &[View], buffers: &[Buffer<u8>]) -> Result<()> {
    for view in views {
        let len = view.length;
        if len <= 12 {
            validate_utf8(view.to_le_bytes().get_unchecked(4..4 + len as usize))?;
        } else {
            let buffer_idx = view.buffer_idx;
            let offset = view.offset;
            let data = buffers.get_unchecked(buffer_idx as usize);

            let start = offset as usize;
            let end = start + len as usize;
            let b = &data.as_slice().get_unchecked(start..end);
            validate_utf8(b)?;
        };
    }

    Ok(())
}
