
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

use common_datavalues2::NullableType;
use common_datavalues2::BooleanType;
use common_exception::Result;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::ColumnRef;

use crate::scalars::{Function2Factory, Function2};
use super::LogicNotFunction2;
use super::LogicAndFunction2;
use super::LogicOrFunction2;
use super::logic2_xor::LogicXorFunction2;
use common_datavalues2::DataTypePtr;

#[derive(Clone)]
pub struct LogicFunction2 {
    op: LogicOperator,
}

#[derive(Debug, Clone)]
pub enum LogicOperator {
    Not,
    And,
    Or,
    Xor,
}

impl LogicFunction2 {
    pub fn register(factory: &mut Function2Factory) {
        factory.register("and2", LogicAndFunction2::desc());
        factory.register("or2", LogicOrFunction2::desc());
        factory.register("not2", LogicNotFunction2::desc());
        factory.register("xor", LogicXorFunction2::desc());
    }

    pub fn try_create(op: LogicOperator) -> Result<Box<dyn Function2>> {
        Ok(Box::new(LogicFunction2 { op }))
    }
}

impl Function2 for LogicFunction2 {
    fn name(&self) -> &str {
        "LogicFunction"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let dt: DataTypePtr = Arc::new(NullableType::create(BooleanType::arc()));
        Ok(dt)
    }

    fn eval(&self, _columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        unimplemented!()
    }

    fn passthrough_null(&self) -> bool {
        match self.op {
            LogicOperator::Or => false,
            _ => true
        }
    }
}

impl std::fmt::Display for LogicFunction2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.op)
    }
}
