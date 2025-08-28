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
use std::sync::Mutex;
use std::time::Duration;

use crate::leveled_store::leveled_map::applier_acquirer::WriterPermit;
use crate::leveled_store::view::StateMachineView;
use crate::sm_v003::OnChange;

pub(crate) struct ApplierData {
    /// Hold a unique permit to serialize all apply operations to the state machine.
    pub(crate) _permit: WriterPermit,

    pub(crate) view: StateMachineView,

    /// Since when to start cleaning expired keys.
    pub(crate) cleanup_start_time: Arc<Mutex<Duration>>,

    pub(crate) on_change_applied: Arc<Option<OnChange>>,
}
