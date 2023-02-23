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

use std::collections::HashMap;

use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::RuntimeFilterId;
use crate::plans::RuntimeFilterSource;
use crate::ScalarExpr;

pub struct RuntimeFilterResult {
    pub runtime_filters: HashMap<RuntimeFilterId, ScalarExpr>,
    // Used by join probe side
    pub predicates: Vec<ScalarExpr>,
}
pub fn create_runtime_filters(join: &mut Join) -> Result<RuntimeFilterResult> {
    todo!()
}

pub fn wrap_filter_to_probe(s_expr: &mut SExpr, predicates: Vec<ScalarExpr>) -> Result<()> {
    let mut probe_side = s_expr.child(0)?.child(0)?.clone();
    let new_filter = Filter {
        predicates,
        is_having: false,
    };
    probe_side = SExpr::create_unary(new_filter.into(), probe_side);
    s_expr
        .child(0)?
        .replace_children(vec![probe_side, s_expr.child(0)?.child(1)?.clone()]);
    Ok(())
}

pub fn wrap_runtime_filter_source_to_build(
    s_expr: &mut SExpr,
    runtime_filters: HashMap<RuntimeFilterId, ScalarExpr>,
) -> Result<()> {
    let new_node = RuntimeFilterSource { runtime_filters };
    let mut build_side = s_expr.child(0)?.child(1)?.clone();
    build_side = SExpr::create_unary(new_node.into(), build_side);
    s_expr
        .child(0)?
        .replace_children(vec![s_expr.child(0)?.child(0)?.clone(), build_side]);
    Ok(())
}
