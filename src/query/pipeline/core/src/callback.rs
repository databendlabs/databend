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

use crate::PlanProfile;

pub struct ExecutionInfo {
    pub res: Result<()>,
    pub profiling: Vec<PlanProfile>,
}

impl ExecutionInfo {
    pub fn create(res: Result<()>, profiling: Vec<PlanProfile>) -> ExecutionInfo {
        ExecutionInfo { res, profiling }
    }
}
