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

use crate::OutputCommonSettings;

// 30% faster lexical_core::write to tmp buf and extend_from_slice
#[inline]
pub fn extend_lexical<N: lexical_core::ToLexical>(n: N, out_buf: &mut Vec<u8>) {
    out_buf.reserve(N::FORMATTED_SIZE_DECIMAL);
    let len0 = out_buf.len();
    unsafe {
        let slice = std::slice::from_raw_parts_mut(
            out_buf.as_mut_ptr().add(len0),
            out_buf.capacity() - len0,
        );
        let len = lexical_core::write(n, slice).len();
        out_buf.set_len(len0 + len);
    }
}

pub trait PrimitiveWithFormat {
    fn write_field(self, buf: &mut Vec<u8>, settings: &OutputCommonSettings);
}

macro_rules! impl_float {
    ($ty:ident) => {
        impl PrimitiveWithFormat for $ty {
            fn write_field(self: $ty, buf: &mut Vec<u8>, settings: &OutputCommonSettings) {
                match self {
                    $ty::INFINITY => buf.extend_from_slice(&settings.inf_bytes),
                    $ty::NEG_INFINITY => {
                        buf.push(b'-');
                        buf.extend_from_slice(&settings.inf_bytes);
                    }
                    _ => {
                        if self.is_nan() {
                            buf.extend_from_slice(&settings.nan_bytes);
                        } else {
                            extend_lexical(self, buf);
                        }
                    }
                }
            }
        }
    };
}

macro_rules! impl_int {
    ($ty:ident) => {
        impl PrimitiveWithFormat for $ty {
            fn write_field(self: $ty, out_buf: &mut Vec<u8>, _settings: &OutputCommonSettings) {
                extend_lexical(self, out_buf);
            }
        }
    };
}

impl_int!(i8);
impl_int!(i16);
impl_int!(i32);
impl_int!(i64);
impl_int!(u8);
impl_int!(u16);
impl_int!(u32);
impl_int!(u64);
impl_float!(f32);
impl_float!(f64);
