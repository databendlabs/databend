// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_planners::Expression;
use common_planners::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExecutePlanWithShuffleAction {
    pub query_id: String,
    pub stage_id: String,
    pub plan: PlanNode,
    pub scatters: Vec<String>,
    pub scatters_action: Expression
}
