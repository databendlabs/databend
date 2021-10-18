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

use common_datavalues::DataValue;

use crate::Expression;

pub trait Literal {
    fn to_literal(&self) -> Expression;
}

impl Literal for &[u8] {
    fn to_literal(&self) -> Expression {
        Expression::create_literal(DataValue::String(Some(self.to_vec())))
    }
}

impl Literal for Vec<u8> {
    fn to_literal(&self) -> Expression {
        Expression::create_literal(DataValue::String(Some(self.clone())))
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident) => {
        #[allow(missing_docs)]
        impl Literal for $TYPE {
            fn to_literal(&self) -> Expression {
                Expression::create_literal(DataValue::$SCALAR(Some(self.clone())))
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

pub fn lit<T: Literal>(n: T) -> Expression {
    n.to_literal()
}
