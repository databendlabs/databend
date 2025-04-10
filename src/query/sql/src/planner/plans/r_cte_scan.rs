// Copyright 2021 Datafuse Labs
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

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;

use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::ColumnSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecursiveCteScan {
    pub(crate) fields: Vec<DataField>,
    pub(crate) table_name: String,
}

impl RecursiveCteScan {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for field in self.fields.iter() {
            used_columns.insert(field.name().parse()?);
        }
        Ok(used_columns)
    }
}

impl Hash for RecursiveCteScan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        for field in self.fields.iter() {
            field.name().hash(state);
        }
    }
}

impl Operator for RecursiveCteScan {
    fn rel_op(&self) -> RelOp {
        RelOp::RecursiveCteScan
    }

    fn arity(&self) -> usize {
        0
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.used_columns()?,
            outer_columns: ColumnSet::new(),
            used_columns: self.used_columns()?,
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Err(ErrorCode::Internal(
            "Cannot compute required property for RecursiveCteScan".to_string(),
        ))
    }
}
