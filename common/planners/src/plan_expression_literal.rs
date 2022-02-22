// Copyright 2021 Datafuse Labs.
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

use common_datavalues::DataValue;

use crate::Expression;

pub trait Literal {
    fn to_literal(&self) -> Expression;
}

impl Literal for &[u8] {
    fn to_literal(&self) -> Expression {
        Expression::create_literal(DataValue::String(self.to_vec()))
    }
}

impl Literal for Vec<u8> {
    fn to_literal(&self) -> Expression {
        Expression::create_literal(DataValue::String(self.clone()))
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SUPER: ident, $SCALAR:ident) => {
        #[allow(missing_docs)]
        impl Literal for $TYPE {
            fn to_literal(&self) -> Expression {
                Expression::create_literal(DataValue::$SCALAR(*self as $SUPER))
            }
        }
    };
}

make_literal!(bool, bool, Boolean);
make_literal!(f32, f64, Float64);
make_literal!(f64, f64, Float64);

make_literal!(i8, i64, Int64);
make_literal!(i16, i64, Int64);
make_literal!(i32, i64, Int64);
make_literal!(i64, i64, Int64);

make_literal!(u8, u64, UInt64);
make_literal!(u16, u64, UInt64);
make_literal!(u32, u64, UInt64);
make_literal!(u64, u64, UInt64);

pub fn lit<T: Literal>(n: T) -> Expression {
    n.to_literal()
}

pub fn lit_null() -> Expression {
    Expression::create_literal(DataValue::Null)
}
