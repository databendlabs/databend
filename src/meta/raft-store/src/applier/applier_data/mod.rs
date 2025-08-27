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

use map_api::mvcc;

use crate::leveled_store::leveled_map::applier_acquirer::WriterPermit;
use crate::leveled_store::leveled_map::leveled_map_data::LeveledMapData;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;
use crate::sm_v003::OnChange;

mod impl_expire_scoped_view;
mod impl_expire_scoped_view_readonly;
mod impl_user_scoped_view;
mod impl_user_scoped_view_readonly;

pub(crate) type StateMachineView = mvcc::View<Namespace, Key, Value, Arc<LeveledMapData>>;

pub(crate) struct ApplierData {
    /// Hold a unique permit to serialize all apply operations to the state machine.
    pub(crate) _permit: WriterPermit,

    pub(crate) view: StateMachineView,

    /// Since when to start cleaning expired keys.
    pub(crate) cleanup_start_time: Arc<Mutex<Duration>>,

    pub(crate) on_change_applied: Arc<Option<OnChange>>,
}
