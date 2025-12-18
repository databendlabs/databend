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

use crate::binary::BinaryColumn;
use crate::binary::BinaryColumnBuilder;
use crate::buffer::Buffer;
use crate::error::Error;
use crate::error::Result;
use crate::types::NativeType;
use crate::types::PrimitiveType;

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
    pub _align: [u128; 0],
}

impl View {
    pub const MAX_INLINE_SIZE: u32 = 12;

    #[inline(always)]
    pub fn as_u128(self) -> u128 {
        unsafe { std::mem::transmute(self) }
    }

    #[inline(always)]
    pub fn as_i128(self) -> i128 {
        unsafe { std::mem::transmute(self) }
    }

    /// Create a new inline view without verifying the length
    ///
    /// # Safety
    ///
    /// It needs to hold that `bytes.len() <= View::MAX_INLINE_SIZE`.
    #[inline]
    pub unsafe fn new_inline_unchecked(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() <= u32::MAX as usize);
        debug_assert!(bytes.len() as u32 <= Self::MAX_INLINE_SIZE);

        let mut view = Self {
            length: bytes.len() as u32,
            ..Default::default()
        };

        let view_ptr = &mut view as *mut _ as *mut u8;

        // SAFETY:
        // - bytes length <= 12,
        // - size_of::<View> == 16
        // - View is laid out as [length, prefix, buffer_idx, offset] (using repr(C))
        // - By grabbing the view_ptr and adding 4, we have provenance over prefix, buffer_idx and
        // offset. (i.e. the same could not be achieved with &mut self.prefix as *mut _ as *mut u8)
        unsafe {
            let inline_data_ptr = view_ptr.add(4);
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), inline_data_ptr, bytes.len());
        }
        view
    }

    /// Create a new inline view
    ///
    /// # Panics
    ///
    /// Panics if the `bytes.len() > View::MAX_INLINE_SIZE`.
    #[inline]
    pub fn new_inline(bytes: &[u8]) -> Self {
        assert!(bytes.len() as u32 <= Self::MAX_INLINE_SIZE);
        unsafe { Self::new_inline_unchecked(bytes) }
    }

    /// Create a new inline view
    ///
    /// # Safety
    ///
    /// It needs to hold that `bytes.len() > View::MAX_INLINE_SIZE`.
    #[inline]
    pub unsafe fn new_noninline_unchecked(bytes: &[u8], buffer_idx: u32, offset: u32) -> Self {
        debug_assert!(bytes.len() <= u32::MAX as usize);
        debug_assert!(bytes.len() as u32 > View::MAX_INLINE_SIZE);

        // SAFETY: The invariant of this function guarantees that this is safe.
        let prefix = unsafe { u32::from_le_bytes(bytes[0..4].try_into().unwrap_unchecked()) };
        Self {
            length: bytes.len() as u32,
            prefix,
            buffer_idx,
            offset,
            ..Default::default()
        }
    }

    #[inline]
    pub fn new_from_bytes(bytes: &[u8], buffer_idx: u32, offset: u32) -> Self {
        debug_assert!(bytes.len() <= u32::MAX as usize);

        // SAFETY: We verify the invariant with the outer if statement
        unsafe {
            if bytes.len() as u32 <= Self::MAX_INLINE_SIZE {
                Self::new_inline_unchecked(bytes)
            } else {
                Self::new_noninline_unchecked(bytes, buffer_idx, offset)
            }
        }
    }

    /// Constructs a byteslice from this view.
    ///
    /// # Safety
    /// Assumes that this view is valid for the given buffers.
    pub unsafe fn get_slice_unchecked<'a>(&'a self, buffers: &'a [Buffer<u8>]) -> &'a [u8] {
        unsafe {
            if self.length <= Self::MAX_INLINE_SIZE {
                let ptr = self as *const View as *const u8;
                std::slice::from_raw_parts(ptr.add(4), self.length as usize)
            } else {
                let data = buffers.get_unchecked(self.buffer_idx as usize);
                let offset = self.offset as usize;
                data.get_unchecked(offset..offset + self.length as usize)
            }
        }
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
    unsafe {
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
}

pub trait CheckUTF8 {
    fn check_utf8(&self) -> Result<()>;
}

impl CheckUTF8 for &[u8] {
    fn check_utf8(&self) -> Result<()> {
        validate_utf8(self)
    }
}

impl CheckUTF8 for Vec<u8> {
    fn check_utf8(&self) -> Result<()> {
        self.as_slice().check_utf8()
    }
}

impl CheckUTF8 for BinaryColumn {
    fn check_utf8(&self) -> Result<()> {
        for bytes in self.iter() {
            bytes.check_utf8()?;
        }
        Ok(())
    }
}

impl CheckUTF8 for BinaryColumnBuilder {
    fn check_utf8(&self) -> Result<()> {
        check_utf8_column(&self.offsets, &self.data)
    }
}

/// # Check if any slice of `values` between two consecutive pairs from `offsets` is invalid `utf8`
fn check_utf8_column(offsets: &[u64], data: &[u8]) -> Result<()> {
    let res: Option<()> = try {
        if offsets.len() == 1 {
            return Ok(());
        }

        if data.is_ascii() {
            return Ok(());
        }

        simdutf8::basic::from_utf8(data).ok()?;

        let last = if let Some(last) = offsets.last() {
            if *last as usize == data.len() {
                return Ok(());
            } else {
                *last as usize
            }
        } else {
            // given `l = data.len()`, this branch is hit iff either:
            // * `offsets = [0, l, l, ...]`, which was covered by `from_utf8(data)` above
            // * `offsets = [0]`, which never happens because offsets.len() == 1 is short-circuited above
            return Ok(());
        };

        // truncate to relevant offsets. Note: `=last` because last was computed skipping the first item
        // following the example: starts = [0, 5]
        let starts = unsafe { offsets.get_unchecked(..=last) };

        let mut any_invalid = false;
        for start in starts {
            let start = *start as usize;

            // Safety: `try_check_offsets_bounds` just checked for bounds
            let b = *unsafe { data.get_unchecked(start) };

            // A valid code-point iff it does not start with 0b10xxxxxx
            // Bit-magic taken from `std::str::is_char_boundary`
            if (b as i8) < -0x40 {
                any_invalid = true
            }
        }
        if any_invalid {
            None?;
        }
    };
    res.ok_or_else(|| Error::oos("invalid utf8"))
}
