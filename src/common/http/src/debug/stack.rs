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

use databend_common_base::dump_backtrace;
use poem::web::Query;
use poem::IntoResponse;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DumpStackRequest {
    wait_for_running_tasks: bool,
}

#[poem::handler]
pub async fn debug_dump_stack(req: Option<Query<DumpStackRequest>>) -> impl IntoResponse {
    dump_backtrace(req.map(|x| x.wait_for_running_tasks).unwrap_or(false))
}
