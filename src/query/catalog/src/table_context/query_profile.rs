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

use std::collections::HashMap;

use databend_common_pipeline::core::PlanProfile;

pub trait TableContextQueryProfile: Send + Sync {
    fn get_queries_profile(&self) -> HashMap<String, Vec<PlanProfile>>;

    fn add_query_profiles(&self, profiles: &HashMap<u32, PlanProfile>);

    fn get_query_profiles(&self) -> Vec<PlanProfile>;
}
