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

use common_exception::Result;

use crate::prelude::*;

pub trait BinarySer {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<()>;
    fn serialize_to_buf<W: bytes::BufMut>(&self, writer: &mut W) -> Result<()>;
}

macro_rules! apply_scalar_ser {
    ($($t: ident),* ) => {
        $(
            impl BinarySer for $t {
                fn serialize<W: std::io::Write>(&self, writer:  &mut W) -> Result<()> {
                    writer.write_scalar(self)
                }

                fn serialize_to_buf<W: bytes::BufMut>(&self, writer: &mut  W) -> Result<()>{
                    writer.write_scalar(self)
                }
            }
        )*
    };
}

// primitive types and boolean
apply_scalar_ser! {u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, bool}

impl BinarySer for String {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        let bytes = self.as_bytes();
        writer.write_uvarint(bytes.len() as u64)?;
        writer.write_all(bytes)?;
        Ok(())
    }

    fn serialize_to_buf<W: bytes::BufMut>(&self, writer: &mut W) -> Result<()> {
        let bytes = self.as_bytes();
        writer.write_uvarint(bytes.len() as u64)?;
        writer.put_slice(bytes);
        Ok(())
    }
}

impl<T> BinarySer for Option<T>
where T: BinarySer
{
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            Some(v) => {
                writer.write_scalar(&1u8)?;
                v.serialize(writer)
            }
            None => writer.write_scalar(&0u8),
        }
    }

    fn serialize_to_buf<W: bytes::BufMut>(&self, writer: &mut W) -> Result<()> {
        match self {
            Some(v) => {
                writer.write_scalar(&1u8)?;
                v.serialize_to_buf(writer)
            }
            None => writer.write_scalar(&0u8),
        }
    }
}
