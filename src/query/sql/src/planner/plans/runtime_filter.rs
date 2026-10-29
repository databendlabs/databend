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

use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;

use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

/// Name of the planner-only predicate that carries a join runtime filter
/// through filter push-down rules: `__runtime_filter(id)(probe_key)`.
pub const RUNTIME_FILTER_FUNC_NAME: &str = "__runtime_filter";

/// A runtime filter built from the build side of a join.
///
/// `probe_key` and `build_key` are the canonical keys of one equi condition
/// of the join (see `JoinEquiCondition::canonical_keys`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JoinRuntimeFilter {
    pub id: usize,
    pub probe_key: ScalarExpr,
    pub build_key: ScalarExpr,
}

/// A runtime filter target that reached a table scan. `probe_key` is the
/// join probe key rewritten over the columns of this scan.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScanRuntimeFilter {
    pub id: usize,
    pub probe_key: ScalarExpr,
}

/// Build the predicate `__runtime_filter(id)(probe_key)`.
pub fn runtime_filter_predicate(id: usize, probe_key: ScalarExpr) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: RUNTIME_FILTER_FUNC_NAME.to_string(),
        params: vec![Scalar::Number(NumberScalar::UInt64(id as u64))],
        arguments: vec![probe_key],
        return_type: Box::new(DataType::Boolean),
    })
}

/// Return the runtime filter id and probe key if `predicate` is a runtime filter predicate.
pub fn as_runtime_filter_predicate(predicate: &ScalarExpr) -> Option<(usize, &ScalarExpr)> {
    let ScalarExpr::FunctionCall(func) = predicate else {
        return None;
    };
    if func.func_name != RUNTIME_FILTER_FUNC_NAME {
        return None;
    }
    let ([param], [probe_key]) = (func.params.as_slice(), func.arguments.as_slice()) else {
        return None;
    };
    let id = usize::try_from(param.get_i64()?).ok()?;
    Some((id, probe_key))
}

pub fn is_runtime_filter_predicate(predicate: &ScalarExpr) -> bool {
    as_runtime_filter_predicate(predicate).is_some()
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Symbol;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;

    #[test]
    fn test_runtime_filter_predicate_round_trip() {
        let column = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "a".to_string(),
                Symbol::new(3),
                Box::new(DataType::Number(NumberDataType::Int32)),
                Visibility::Visible,
            )
            .build(),
        });
        let predicate = runtime_filter_predicate(7, column.clone());
        assert_eq!(as_runtime_filter_predicate(&predicate), Some((7, &column)));
        assert!(!is_runtime_filter_predicate(&column));
    }
}
