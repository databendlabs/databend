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

use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use educe::Educe;

use crate::optimizer::SExpr;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;

#[derive(Debug, PartialEq, Clone, Educe)]
#[educe(Eq, Hash)]
pub struct Recluster {
    pub catalog: String,
    pub database: String,
    pub table: String,

    pub limit: Option<usize>,
    #[educe(Hash(ignore))]
    pub filters: Option<Filters>,
}

impl Operator for Recluster {
    fn rel_op(&self) -> RelOp {
        RelOp::Recluster
    }
}

pub fn set_update_stream_columns(s_expr: &SExpr) -> databend_common_exception::Result<SExpr> {
    match s_expr.plan() {
        RelOperator::Scan(scan) if scan.table_index == 0 => {
            let mut scan = scan.clone();
            scan.set_update_stream_columns(true);
            Ok(SExpr::create_leaf(Arc::new(scan.into())))
        }
        _ => {
            let mut children = Vec::with_capacity(s_expr.arity());
            for child in s_expr.children() {
                let child = set_update_stream_columns(child)?;
                children.push(Arc::new(child));
            }
            Ok(s_expr.replace_children(children))
        }
    }
}
