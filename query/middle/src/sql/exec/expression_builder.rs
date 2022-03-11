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

use common_exception::Result;
use common_planners::Expression;

use crate::sql::BoundVariable;
use crate::sql::Metadata;
use crate::sql::ScalarExpr;

pub struct ExpressionBuilder<'a> {
    metadata: &'a Metadata,
}

impl<'a> ExpressionBuilder<'a> {
    pub fn create(metadata: &'a Metadata) -> Self {
        ExpressionBuilder { metadata }
    }

    pub fn build(&self, scalar: &ScalarExpr) -> Result<Expression> {
        match scalar {
            ScalarExpr::BoundVariable(expr) => self.build_bound_variable(expr),
        }
    }

    fn build_bound_variable(&self, bound_variable: &BoundVariable) -> Result<Expression> {
        let column_entry = self.metadata.column(bound_variable.index);
        let result = Expression::Column(column_entry.name.clone());
        Ok(result)
    }
}
