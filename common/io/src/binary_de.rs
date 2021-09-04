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

pub trait BinaryDe: Sized {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self>;
}

macro_rules! apply_scalar_de {
    ($($t: ident),* ) => {
        $(
            impl BinaryDe for $t {
                fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self> {
                    reader.read_scalar()
                }
            }
        )*
    };
}

// primitive types and boolean
apply_scalar_de! {u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, bool}

impl BinaryDe for Vec<u8> {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let str_len = reader.read_uvarint()? as usize;
        let mut buffer = vec![0_u8; str_len];
        reader.read_exact(buffer.as_mut())?;
        Ok(buffer)
    }
}

impl<T> BinaryDe for Option<T>
where T: BinaryDe
{
    fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let is_some: u8 = reader.read_scalar()?;
        if is_some > 0 {
            let t = T::deserialize(reader)?;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }
}
