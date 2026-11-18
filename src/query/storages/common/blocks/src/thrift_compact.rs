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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

const MAX_VARINT32_BYTES: usize = 5;
const MAX_VARINT64_BYTES: usize = 10;
pub const DEFAULT_MAX_THRIFT_DEPTH: usize = 64;

/// Wire types in the Thrift Compact Protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactType {
    /// Struct boolean field whose value is encoded in the field header nibble.
    InlineBool(bool),
    /// Collection boolean element stored as a 1-byte value (`0x00..=0x02`).
    Bool,
    I8,
    I16,
    I32,
    I64,
    Double,
    Binary,
    List,
    Set,
    Map,
    Struct,
    Uuid,
}

impl CompactType {
    fn from_common_nibble(nibble: u8) -> Result<Self> {
        match nibble {
            0x03 => Ok(Self::I8),
            0x04 => Ok(Self::I16),
            0x05 => Ok(Self::I32),
            0x06 => Ok(Self::I64),
            0x07 => Ok(Self::Double),
            0x08 => Ok(Self::Binary),
            0x09 => Ok(Self::List),
            0x0A => Ok(Self::Set),
            0x0B => Ok(Self::Map),
            0x0C => Ok(Self::Struct),
            0x0D => Ok(Self::Uuid),
            other => Err(thrift_err(format!(
                "unknown Thrift compact wire type {other}"
            ))),
        }
    }

    fn from_field_nibble(nibble: u8) -> Result<Self> {
        match nibble {
            0x01 => Ok(Self::InlineBool(true)),
            0x02 => Ok(Self::InlineBool(false)),
            other => Self::from_common_nibble(other),
        }
    }

    fn from_elem_nibble(nibble: u8) -> Result<Self> {
        match nibble {
            // Both 0x01 and 0x02 are valid for collection booleans per the Thrift compact spec.
            0x01 | 0x02 => Ok(Self::Bool),
            other => Self::from_common_nibble(other),
        }
    }

    fn min_wire_size(self) -> usize {
        match self {
            Self::InlineBool(_) => 0,
            Self::Double => 8,
            Self::Uuid => 16,
            _ => 1,
        }
    }
}

/// Bounded, zero-allocation Thrift Compact Protocol reader over an in-memory byte slice.
///
/// Binary and container lengths are validated against the remaining slice before advancing,
/// avoiding eager heap allocations from untrusted length prefixes.
pub struct ThriftCompactReader<'a> {
    buf: &'a [u8],
    depth_budget: usize,
}

impl<'a> ThriftCompactReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            depth_budget: DEFAULT_MAX_THRIFT_DEPTH,
        }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len()
    }

    pub fn read_byte(&mut self) -> Result<u8> {
        let (&b, rest) = self
            .buf
            .split_first()
            .ok_or_else(|| thrift_err("unexpected EOF"))?;
        self.buf = rest;
        Ok(b)
    }

    pub fn read_slice(&mut self, len: usize) -> Result<&'a [u8]> {
        let (head, tail) = self
            .buf
            .split_at_checked(len)
            .ok_or_else(|| thrift_err("unexpected EOF"))?;
        self.buf = tail;
        Ok(head)
    }

    pub fn read_varint32(&mut self) -> Result<u32> {
        let mut result = 0u32;
        let mut shift = 0u32;
        for _ in 0..MAX_VARINT32_BYTES {
            let b = self.read_byte()?;
            result |= u32::from(b & 0x7F) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(thrift_err("varint32 exceeds 5 bytes"))
    }

    pub fn read_zigzag_i32(&mut self) -> Result<i32> {
        let n = self.read_varint32()?;
        Ok(((n >> 1) as i32) ^ -(n as i32 & 1))
    }

    pub fn read_bool(&mut self, ty: CompactType) -> Result<bool> {
        match ty {
            CompactType::InlineBool(v) => Ok(v),
            CompactType::Bool => match self.read_byte()? {
                0x01 => Ok(true),
                0x00 | 0x02 => Ok(false),
                other => Err(thrift_err(format!("invalid boolean value {other}"))),
            },
            other => Err(unexpected_type(other, "Bool")),
        }
    }

    pub fn read_i32(&mut self, ty: CompactType) -> Result<i32> {
        if ty != CompactType::I32 {
            return Err(unexpected_type(ty, "I32"));
        }
        self.read_zigzag_i32()
    }

    pub fn read_bytes(&mut self, ty: CompactType) -> Result<&'a [u8]> {
        if ty != CompactType::Binary {
            return Err(unexpected_type(ty, "Binary"));
        }
        let len = usize::try_from(self.read_varint32()?)
            .map_err(|_| thrift_err("binary length overflow"))?;
        self.read_slice(len)
    }

    pub fn read_root_struct(
        &mut self,
        mut on_field: impl FnMut(&mut Self, i16, CompactType) -> Result<()>,
    ) -> Result<()> {
        self.with_depth(|r| {
            let mut last_field_id = 0i16;
            while let Some((field_id, ty)) = r.read_field_begin(last_field_id)? {
                on_field(r, field_id, ty)?;
                last_field_id = field_id;
            }
            Ok(())
        })
    }

    pub fn read_struct(
        &mut self,
        ty: CompactType,
        on_field: impl FnMut(&mut Self, i16, CompactType) -> Result<()>,
    ) -> Result<()> {
        if ty != CompactType::Struct {
            return Err(unexpected_type(ty, "Struct"));
        }
        self.read_root_struct(on_field)
    }

    pub fn skip_value(&mut self, ty: CompactType) -> Result<()> {
        match ty {
            CompactType::InlineBool(_) => Ok(()),
            CompactType::Bool => self.read_bool(CompactType::Bool).map(|_| ()),
            CompactType::I8 => self.read_byte().map(|_| ()),
            CompactType::I16 | CompactType::I32 => self.skip_varint(MAX_VARINT32_BYTES),
            CompactType::I64 => self.skip_varint(MAX_VARINT64_BYTES),
            CompactType::Double => self.read_slice(8).map(|_| ()),
            CompactType::Binary => self.read_bytes(CompactType::Binary).map(|_| ()),
            CompactType::List | CompactType::Set => self.skip_list_or_set(),
            CompactType::Map => self.skip_map(),
            CompactType::Struct => self.read_root_struct(|r, _, ty| r.skip_value(ty)),
            CompactType::Uuid => self.read_slice(16).map(|_| ()),
        }
    }

    fn read_field_begin(&mut self, last_field_id: i16) -> Result<Option<(i16, CompactType)>> {
        let header = self.read_byte()?;
        let low = header & 0x0F;
        if low == 0 {
            return Ok(None);
        }
        let ty = CompactType::from_field_nibble(low)?;
        let delta = (header & 0xF0) >> 4;
        let field_id = if delta != 0 {
            last_field_id
                .checked_add(i16::from(delta))
                .ok_or_else(|| thrift_err("field id delta overflow"))?
        } else {
            i16::try_from(self.read_zigzag_i32()?)
                .map_err(|_| thrift_err("i16 field id out of range"))?
        };
        Ok(Some((field_id, ty)))
    }

    fn skip_varint(&mut self, max_bytes: usize) -> Result<()> {
        for _ in 0..max_bytes {
            if self.read_byte()? & 0x80 == 0 {
                return Ok(());
            }
        }
        Err(thrift_err("varint exceeds maximum length"))
    }

    fn skip_list_or_set(&mut self) -> Result<()> {
        self.with_depth(|r| {
            let header = r.read_byte()?;
            // Some Parquet writers emit 0x00 for an empty list header.
            if header == 0 {
                return Ok(());
            }
            let elem_ty = CompactType::from_elem_nibble(header & 0x0F)?;
            let high = (header & 0xF0) >> 4;
            let raw_count = if high != 15 {
                u32::from(high)
            } else {
                r.read_varint32()?
            };
            let count = r.check_container_count(raw_count, elem_ty.min_wire_size())?;
            for _ in 0..count {
                r.skip_value(elem_ty)?;
            }
            Ok(())
        })
    }

    fn skip_map(&mut self) -> Result<()> {
        self.with_depth(|r| {
            let raw_count = r.read_varint32()?;
            if raw_count == 0 {
                return Ok(());
            }
            let types = r.read_byte()?;
            let key_ty = CompactType::from_elem_nibble((types & 0xF0) >> 4)?;
            let val_ty = CompactType::from_elem_nibble(types & 0x0F)?;
            let count = r.check_container_count(
                raw_count,
                key_ty.min_wire_size() + val_ty.min_wire_size(),
            )?;
            for _ in 0..count {
                r.skip_value(key_ty)?;
                r.skip_value(val_ty)?;
            }
            Ok(())
        })
    }

    fn check_container_count(&self, raw_count: u32, min_elem_bytes: usize) -> Result<usize> {
        let count_i32 = i32::try_from(raw_count)
            .map_err(|_| thrift_err("negative or oversized container length"))?;
        let count =
            usize::try_from(count_i32).map_err(|_| thrift_err("negative container length"))?;
        let min_bytes = count
            .checked_mul(min_elem_bytes)
            .ok_or_else(|| thrift_err("container byte size overflow"))?;
        if min_bytes > self.buf.len() {
            return Err(thrift_err("container length exceeds remaining bytes"));
        }
        Ok(count)
    }

    fn with_depth<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if self.depth_budget == 0 {
            return Err(thrift_err("Thrift recursion depth limit exceeded"));
        }
        self.depth_budget -= 1;
        let res = f(self);
        self.depth_budget += 1;
        res
    }
}

fn thrift_err(msg: impl Into<String>) -> ErrorCode {
    ErrorCode::ParquetFileInvalid(format!("invalid parquet page header: {}", msg.into()))
}

fn unexpected_type(actual: CompactType, expected: &'static str) -> ErrorCode {
    thrift_err(format!(
        "unexpected Thrift field type {actual:?}, expected {expected}"
    ))
}
