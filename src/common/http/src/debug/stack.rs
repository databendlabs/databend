// Copyright 2023 Datafuse Labs.
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

use std::fmt::Write;

use poem::web::Query;
use poem::IntoResponse;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DumpStackRequest {
    wait_for_running_tasks: bool,
}

#[poem::handler]
pub async fn debug_dump_stack(req: Option<Query<DumpStackRequest>>) -> impl IntoResponse {
    let tree =
        async_backtrace::taskdump_tree(req.map(|x| x.wait_for_running_tasks).unwrap_or(false));

    let mut string = String::new();

    let mut first = true;
    for line in tree.lines() {
        if let Some(first_char) = line.chars().next() {
            if !first_char.is_ascii_whitespace() {
                if !first {
                    writeln!(string).unwrap();
                }

                first = false;
            }
        }

        writeln!(string, "{}", line).unwrap();
    }

    string
}
