// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::sessions::session::MutableStatus;
use crate::sessions::Session;
use crate::sessions::Settings;

pub struct ProcessInfo {
    pub id: String,
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
        status.context_shared.as_ref().and_then(|context_shared| {
            context_shared
                .running_query
                .read()
                .as_ref()
                .map(Clone::clone)
        })
    }
}
