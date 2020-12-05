// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::DataValue;
use crate::planners::ExpressionPlan;

pub trait IConstant {
    fn to_constant(&self) -> ExpressionPlan;
}

impl IConstant for &str {
    fn to_constant(&self) -> ExpressionPlan {
        ExpressionPlan::Constant(DataValue::String(Some(self.to_string())))
    }
}

impl IConstant for String {
    fn to_constant(&self) -> ExpressionPlan {
        ExpressionPlan::Constant(DataValue::String(Some(self.clone())))
    }
}

macro_rules! make_constant {
    ($TYPE:ty, $SCALAR:ident) => {
        #[allow(missing_docs)]
        impl IConstant for $TYPE {
            fn to_constant(&self) -> ExpressionPlan {
                ExpressionPlan::Constant(DataValue::$SCALAR(Some(self.clone())))
            }
        }
    };
}

make_constant!(bool, Boolean);
make_constant!(f32, Float32);
make_constant!(f64, Float64);
make_constant!(i8, Int8);
make_constant!(i16, Int16);
make_constant!(i32, Int32);
make_constant!(i64, Int64);
make_constant!(u8, UInt8);
make_constant!(u16, UInt16);
make_constant!(u32, UInt32);
make_constant!(u64, UInt64);

pub fn constant<T: IConstant>(n: T) -> ExpressionPlan {
    n.to_constant()
}
