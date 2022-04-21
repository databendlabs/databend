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

use crate::sql::exec::util::format_field_name;
use crate::sql::plans::Scalar;
use crate::sql::IndexType;
use crate::sql::Metadata;

pub struct ExpressionBuilder<'a> {
    metadata: &'a Metadata,
}

impl<'a> ExpressionBuilder<'a> {
    pub fn create(metadata: &'a Metadata) -> Self {
        ExpressionBuilder { metadata }
    }

    pub fn build(&self, scalar: &Scalar) -> Result<Expression> {
        match scalar {
            Scalar::ColumnRef { index, .. } => self.build_column_ref(*index),
        }
    }

    pub fn build_column_ref(&self, index: IndexType) -> Result<Expression> {
        let column = self.metadata.column(index);
        Ok(Expression::Column(format_field_name(
            column.name.as_str(),
            index,
        )))
    }
}
