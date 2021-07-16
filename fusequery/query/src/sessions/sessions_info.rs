// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::sessions::ProcessInfo;
use crate::sessions::Session;
use crate::sessions::SessionManager;

impl SessionManager {
    pub fn processes_info(self: &Arc<Self>) -> Vec<ProcessInfo> {
        self.active_sessions
            .read()
            .values()
            .map(Session::process_info)
            .collect::<Vec<_>>()
    }
}
