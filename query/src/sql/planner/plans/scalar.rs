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

use std::any::Any;

use common_datavalues::BooleanType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataTypePtr;
use common_datavalues::DataValue;

use crate::sql::planner::binder::ScalarExpr;
use crate::sql::IndexType;

#[derive(PartialEq, Clone)]
pub enum Scalar {
    ColumnRef {
        index: IndexType,
        data_type: DataTypeImpl,
        nullable: bool,
    },
    Equal {
        left: Box<Scalar>,
        right: Box<Scalar>,
    },
    AggregateFunction {
        func_name: String,
        distinct: bool,
        params: Vec<DataValue>,
        args: Vec<Scalar>,
        data_type: DataTypePtr,
        nullable: bool,
    },
}

impl ScalarExpr for Scalar {
    fn data_type(&self) -> (DataTypeImpl, bool) {
        match &self {
            Scalar::ColumnRef {
                data_type,
                nullable,
                ..
            } => (data_type.clone(), *nullable),
            Scalar::Equal { .. } => (BooleanType::arc(), false),
            Scalar::AggregateFunction {
                data_type,
                nullable,
                ..
            } => (data_type.clone(), *nullable),
        }
    }

    fn contains_aggregate(&self) -> bool {
        false
    }

    fn contains_subquery(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
