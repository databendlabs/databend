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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::sql::optimizer::ColumnSet;
use crate::sql::IndexType;

/// Scalar operator
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarExpr {
    BoundVariable(BoundVariable),
}

impl ScalarExpr {
    pub fn data_type(&self) -> Result<DataTypePtr> {
        match self {
            ScalarExpr::BoundVariable(BoundVariable { data_type, .. }) => Ok(data_type.clone()),
        }
    }

    pub fn nullable(&self) -> bool {
        match self {
            ScalarExpr::BoundVariable(BoundVariable { nullable, .. }) => *nullable,
        }
    }

    pub fn used_columns(&self) -> ColumnSet {
        match self {
            ScalarExpr::BoundVariable(scalar) => scalar.used_columns(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundVariable {
    pub index: IndexType,
    pub data_type: DataTypePtr,
    pub nullable: bool,
}

impl BoundVariable {
    pub fn used_columns(&self) -> ColumnSet {
        ColumnSet::from([self.index])
    }
}
