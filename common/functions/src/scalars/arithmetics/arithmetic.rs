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

use crate::scalars::ArithmeticDivFunction;
use crate::scalars::ArithmeticIntDivFunction;
use crate::scalars::ArithmeticMinusFunction;
use crate::scalars::ArithmeticModuloFunction;
use crate::scalars::ArithmeticMulFunction;
use crate::scalars::ArithmeticNegateFunction;
use crate::scalars::ArithmeticPlusFunction;
use crate::scalars::FunctionFactory;

#[derive(Clone)]
pub struct ArithmeticFunction;

impl ArithmeticFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register_typed("negate", ArithmeticNegateFunction::desc());
        factory.register_typed("+", ArithmeticPlusFunction::desc());
        factory.register_typed("plus", ArithmeticPlusFunction::desc());
        factory.register_typed("-", ArithmeticMinusFunction::desc());
        factory.register_typed("minus", ArithmeticMinusFunction::desc());
        factory.register_typed("*", ArithmeticMulFunction::desc());
        factory.register_typed("multiply", ArithmeticMulFunction::desc());
        factory.register_typed("/", ArithmeticDivFunction::desc());
        factory.register_typed("divide", ArithmeticDivFunction::desc());
        factory.register_typed("div", ArithmeticIntDivFunction::desc());
        factory.register_typed("%", ArithmeticModuloFunction::desc());
        factory.register_typed("modulo", ArithmeticModuloFunction::desc());
        factory.register_typed("mod", ArithmeticModuloFunction::desc());
    }
}
