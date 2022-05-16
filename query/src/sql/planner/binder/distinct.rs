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

use common_exception::Result;

use crate::sql::binder::Binder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;

impl<'a> Binder {
    pub(super) fn bind_distinct(&self, bind_context: &BindContext, child: SExpr) -> Result<SExpr> {
        // Like aggregate, we just use scalar directly.
        // Current it does not support distinct with group by. 
        let group_items: Vec<Scalar> = bind_context
            .all_column_bindings()
            .iter()
            .map(|v| {
                v.scalar
                    .clone()
                    .map(|scalar| *scalar)
                    .unwrap_or_else(|| Scalar::BoundColumnRef(BoundColumnRef { column: v.clone() }))
            })
            .collect();

        println!("---- bind distinct, group items:{:?}", group_items);    
        let distinct_plan = AggregatePlan {
            group_items,
            aggregate_functions: vec![],
        };

        Ok(SExpr::create_unary(distinct_plan.into(), child))
    }
}