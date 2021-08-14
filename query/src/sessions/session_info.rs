// Copyright 2020 Datafuse Labs.
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

use std::net::SocketAddr;
use std::sync::Arc;

use crate::sessions::session::MutableStatus;
use crate::sessions::Session;
use crate::sessions::Settings;

pub struct ProcessInfo {
    pub id: String,
    pub typ: String,
    pub state: String,
    pub database: String,
    #[allow(unused)]
    pub settings: Arc<Settings>,
    pub client_address: Option<SocketAddr>,
    pub session_extra_info: Option<String>,
}

impl Session {
    pub fn process_info(self: &Arc<Self>) -> ProcessInfo {
        let session_mutable_state = self.mutable_state.lock();
        self.to_process_info(&session_mutable_state)
    }

    fn to_process_info(self: &Arc<Self>, status: &MutableStatus) -> ProcessInfo {
        ProcessInfo {
            id: self.id.clone(),
            typ: self.typ.clone(),
            state: self.process_state(status),
            database: status.current_database.clone(),
            settings: status.session_settings.clone(),
            client_address: status.client_host,
            session_extra_info: self.process_extra_info(status),
        }
    }

    fn process_state(self: &Arc<Self>, status: &MutableStatus) -> String {
        match status.context_shared {
            _ if status.abort => String::from("Aborting"),
            None => String::from("Idle"),
            Some(_) => String::from("Query"),
        }
    }

    fn process_extra_info(self: &Arc<Self>, status: &MutableStatus) -> Option<String> {
        match self.typ.as_str() {
            "RPCSession" => Session::rpc_extra_info(status),
            _ => Session::query_extra_info(status),
        }
    }

    fn rpc_extra_info(status: &MutableStatus) -> Option<String> {
        let context_shared = status.context_shared.as_ref();
        context_shared.map(|_| String::from("Partial cluster query stage"))
    }

    fn query_extra_info(status: &MutableStatus) -> Option<String> {
        status.context_shared.as_ref().and_then(|context_shared| {
            context_shared
                .running_query
                .read()
                .as_ref()
                .map(Clone::clone)
        })
    }
}
