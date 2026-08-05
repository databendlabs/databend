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

use databend_common_exception::Result;

use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::ScalarExpr;

pub fn get_join_predicates(join: &Join) -> Result<Vec<ScalarExpr>> {
    Ok(join
        .equi_conditions
        .iter()
        .map(|equi_condition| {
            Ok(ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "eq".to_string(),
                params: vec![],
                arguments: vec![equi_condition.left.clone(), equi_condition.right.clone()],
            }))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .chain(join.non_equi_conditions.clone())
        .collect())
}
