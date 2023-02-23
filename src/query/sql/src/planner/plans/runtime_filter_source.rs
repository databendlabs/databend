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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use common_catalog::table_context::TableContext;

use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RuntimeFilterId {
    id: String,
}

impl RuntimeFilterId {
    pub fn new(id: IndexType) -> Self {
        RuntimeFilterId { id: id.to_string() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RuntimeFilterSource {
    pub runtime_filters: BTreeMap<RuntimeFilterId, ScalarExpr>,
}

impl Operator for RuntimeFilterSource {
    fn rel_op(&self) -> RelOp {
        RelOp::RuntimeFilterSource
    }

    fn derive_relational_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> common_exception::Result<RelationalProperty> {
        todo!()
    }

    fn derive_physical_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> common_exception::Result<PhysicalProperty> {
        todo!()
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        child_index: usize,
        required: &RequiredProperty,
    ) -> common_exception::Result<RequiredProperty> {
        todo!()
    }
}
