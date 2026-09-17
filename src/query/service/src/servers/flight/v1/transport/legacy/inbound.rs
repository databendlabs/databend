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

use std::sync::Arc;

use arrow_flight::FlightData;
use async_channel::Receiver;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;

use crate::pipelines::executor::WatchNotify;

/// Raw do_get response stream with its transport cancellation handle.
pub struct LegacyInbound {
    notify: Arc<WatchNotify>,
    receiver: Receiver<Result<FlightData>>,
}

impl LegacyInbound {
    pub fn create(notify: Arc<WatchNotify>, receiver: Receiver<Result<FlightData>>) -> Self {
        Self { notify, receiver }
    }

    pub async fn recv(&self) -> Result<Option<FlightData>> {
        match self.receiver.recv().await {
            Err(_) => Ok(None),
            Ok(result) => result.map(Some),
        }
    }

    pub fn close(&self) {
        self.receiver.close();
        self.notify.notify_waiters();
    }
}

impl Drop for LegacyInbound {
    fn drop(&mut self) {
        drop_guard(move || self.close())
    }
}
