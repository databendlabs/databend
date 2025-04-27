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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::sessions::SessionManager;

pub static GET_RUNNING_QUERY_DUMP: &str = "/actions/get_running_query_dump";

pub async fn get_running_query_dump(query_id: String) -> Result<String> {
    match SessionManager::instance().get_running_graph_dump(&query_id) {
        Ok(running_graph_dump) => Ok(running_graph_dump),
        Err(cause) => match cause.code() == ErrorCode::UNKNOWN_QUERY {
            false => Err(cause),
            true => DataExchangeManager::instance().get_running_query_graph_dump(&query_id),
        },
    }
}
