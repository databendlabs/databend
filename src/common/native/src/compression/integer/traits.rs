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

use std::cmp::Ordering;
use std::hash::Hash;

use databend_common_column::types::NativeType;
use databend_common_column::types::i256;
use databend_common_column::types::months_days_micros;

pub trait IntegerType: NativeType + PartialOrd + Hash + Eq {
    fn compare_i64(&self, i: i64) -> Ordering;
    const USE_COMMON_COMPRESSION: bool;
}

macro_rules! integer_type {
    ($type:ty) => {
        impl IntegerType for $type {
            fn compare_i64(&self, i: i64) -> Ordering {
                (*self as i64).cmp(&i)
            }
            const USE_COMMON_COMPRESSION: bool = false;
        }
    };
}

integer_type!(u8);
integer_type!(u16);
integer_type!(u32);
integer_type!(u64);
integer_type!(i8);
integer_type!(i16);
integer_type!(i32);
integer_type!(i64);
integer_type!(i128);

impl IntegerType for i256 {
    fn compare_i64(&self, i: i64) -> Ordering {
        self.0.as_i64().cmp(&i)
    }
    const USE_COMMON_COMPRESSION: bool = false;
}

// pub struct months_days_micros(pub i128);
impl IntegerType for months_days_micros {
    fn compare_i64(&self, i: i64) -> Ordering {
        (self.0 as i64).cmp(&i)
    }
    const USE_COMMON_COMPRESSION: bool = true;
}
