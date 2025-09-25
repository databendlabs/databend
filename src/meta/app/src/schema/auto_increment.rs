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

use databend_common_expression::AutoIncrementExpr;

use crate::principal::AutoIncrementKey;
use crate::tenant::Tenant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetAutoIncrementNextValueReq {
    pub tenant: Tenant,
    // Information describing the AutoIncrement
    pub expr: AutoIncrementExpr,
    // AutoIncrement unique key information, including table id and column id
    pub key: AutoIncrementKey,
    // get the number of steps at one time
    pub count: u64,
}

/// The collection of auto increment value in range `[start, end)`, e.g.:
/// `start + i * step` where `start + i + step < end`,
/// or `[start + 0 * step, start + 1 * step, ...)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetAutoIncrementNextValueReply {
    /// The first value in the auto increment, inclusive.
    pub start: u64,
    // step has no use until now
    pub step: i64,
    /// The right bound, exclusive.
    pub end: u64,
}
