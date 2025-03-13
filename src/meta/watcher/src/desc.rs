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

use crate::event_filter::EventFilter;
use crate::id::WatcherId;
use crate::type_config::TypeConfig;
use crate::KeyRange;

/// Descriptor for a watcher that monitors key-value change events.
///
/// A `WatchDesc` defines the scope and filtering criteria for a watcher,
/// specifying which key range to observe and what types of events
/// (updates, deletes) to receive notifications for.
#[derive(Clone, Debug)]
pub struct WatchDesc<C>
where C: TypeConfig
{
    /// Unique identifier for this watcher instance.
    pub watcher_id: WatcherId,

    /// Event filter that determines which event types (update/delete)
    /// this watcher should receive.
    pub interested: EventFilter,

    /// The range of keys this watcher is monitoring.
    /// Only changes to keys within this range will trigger notifications.
    pub key_range: KeyRange<C>,
}

impl<C> WatchDesc<C>
where C: TypeConfig
{
    pub(crate) fn new(id: WatcherId, interested: EventFilter, key_range: KeyRange<C>) -> Self {
        Self {
            watcher_id: id,
            interested,
            key_range,
        }
    }
}
