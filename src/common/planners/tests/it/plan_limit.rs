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

use std::sync::Arc;

use common_exception::Result;
use common_planners::*;

#[test]
fn test_limit_plan() -> Result<()> {
    use pretty_assertions::assert_eq;

    let limit = PlanNode::Limit(LimitPlan {
        n: Some(33),
        offset: 0,
        input: Arc::from(PlanBuilder::empty().build()?),
    });
    let expect = "Limit: 33";
    let actual = format!("{:?}", limit);
    assert_eq!(expect, actual);
    Ok(())
}
