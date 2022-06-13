// Copyright 2022 Datafuse Labs.
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

use std::time::Duration;
use std::time::Instant;

use crate::sessions::SessionRef;

#[derive(PartialEq)]
pub enum ExpiringState {
    InUse(String),
    // return Duration, so user can choose to use Systime or Instance
    Idle { idle_time: Duration },
    Aborted { need_cleanup: bool },
}

pub trait Expirable {
    fn expire_state(&self) -> ExpiringState;
    fn on_expire(&self);
}

impl Expirable for SessionRef {
    fn expire_state(&self) -> ExpiringState {
        if self.is_aborting() {
            ExpiringState::Aborted {
                need_cleanup: false,
            }
        } else {
            match self.get_current_query_id() {
                None => {
                    let status = self.get_status();
                    let status = status.read();
                    ExpiringState::Idle {
                        idle_time: Instant::now() - status.last_access(),
                    }
                }
                Some(query_id) => ExpiringState::InUse(query_id),
            }
        }
    }

    fn on_expire(&self) {}
}
