use std::hash::Hash;

use crate::arrow::types::i256;
use crate::arrow::types::NativeType;

pub trait IntegerType: NativeType + PartialOrd + Hash + Eq {
    fn as_i64(&self) -> i64;
}

macro_rules! integer_type {
    ($type:ty) => {
        impl IntegerType for $type {
            fn as_i64(&self) -> i64 {
                *self as i64
            }
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
// integer_type!(days_ms);
// integer_type!(months_days_ns);

impl IntegerType for i128 {
    fn as_i64(&self) -> i64 {
        *self as i64
    }
}
impl IntegerType for i256 {
    fn as_i64(&self) -> i64 {
        self.0.as_i64()
    }
}
