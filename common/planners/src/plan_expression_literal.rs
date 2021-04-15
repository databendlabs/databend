// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValue;

use crate::ExpressionPlan;

pub trait ILiteral {
    fn to_literal(&self) -> ExpressionPlan;
}

impl ILiteral for &str {
    fn to_literal(&self) -> ExpressionPlan {
        ExpressionPlan::Literal(DataValue::Utf8(Some(self.to_string())))
    }
}

impl ILiteral for String {
    fn to_literal(&self) -> ExpressionPlan {
        ExpressionPlan::Literal(DataValue::Utf8(Some(self.clone())))
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident) => {
        #[allow(missing_docs)]
        impl ILiteral for $TYPE {
            fn to_literal(&self) -> ExpressionPlan {
                ExpressionPlan::Literal(DataValue::$SCALAR(Some(self.clone())))
            }
        }
    };
}

make_literal!(bool, Boolean);
make_literal!(f32, Float32);
make_literal!(f64, Float64);
make_literal!(i8, Int8);
make_literal!(i16, Int16);
make_literal!(i32, Int32);
make_literal!(i64, Int64);
make_literal!(u8, UInt8);
make_literal!(u16, UInt16);
make_literal!(u32, UInt32);
make_literal!(u64, UInt64);

pub fn lit<T: ILiteral>(n: T) -> ExpressionPlan {
    n.to_literal()
}
