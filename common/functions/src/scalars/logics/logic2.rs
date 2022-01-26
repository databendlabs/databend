// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_datavalues2::BooleanType;
use common_datavalues2::ColumnRef;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::DataTypePtr;
use common_datavalues2::NullableType;
use common_exception::Result;

use super::logic2_xor::LogicXorFunction2;
use super::LogicAndFunction2;
use super::LogicNotFunction2;
use super::LogicOrFunction2;
use crate::scalars::Function2;
use crate::scalars::Function2Factory;

#[derive(Clone)]
pub struct LogicFunction2;

impl LogicFunction2 {
    pub fn register(factory: &mut Function2Factory) {
        factory.register("and2", LogicAndFunction2::desc());
        factory.register("or2", LogicOrFunction2::desc());
        factory.register("not2", LogicNotFunction2::desc());
        factory.register("xor", LogicXorFunction2::desc());
    }
}
