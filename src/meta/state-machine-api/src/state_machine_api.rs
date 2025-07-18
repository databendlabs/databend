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

use map_api::MapApi;

use crate::ExpireKey;
use crate::SeqV;
use crate::UserKey;

/// The API a state machine implements.
///
/// The state machine is responsible for managing the application's persistent state,
/// including application kv data and expired key data.
pub trait StateMachineApi<SysData>: Send + Sync {
    /// The map that stores application data.
    type UserMap: MapApi<UserKey> + 'static;

    /// Returns a reference to the map that stores application data.
    ///
    /// This method provides read-only access to the underlying key-value store
    /// that contains the application's persistent state, including application kv data and expired key data.
    fn user_map(&self) -> &Self::UserMap;

    /// Returns a mutable reference to the map that stores application data.
    ///
    /// This method provides read-write access to the underlying key-value store
    /// that contains the application's persistent state, including application kv data and expired key data.
    /// Changes made through this reference will be persisted according to the state machine's replication
    /// protocol.
    fn user_map_mut(&mut self) -> &mut Self::UserMap;

    /// The map that stores expired key data.
    type ExpireMap: MapApi<ExpireKey> + 'static;

    /// Returns a reference to the map that stores expired key data.
    fn expire_map(&self) -> &Self::ExpireMap;

    /// Returns a mutable reference to the map that stores expired key data.
    fn expire_map_mut(&mut self) -> &mut Self::ExpireMap;

    /// Returns a mutable reference to the system data.
    ///
    /// This method provides read-write access to the system data, which includes
    /// metadata about the state machine and its configuration.
    fn sys_data_mut(&mut self) -> &mut SysData;

    /// Notify subscribers of a key-value change applied to the state machine.
    ///
    /// Called after a change is committed, but before it is guaranteed persisted.
    /// The change may be replayed on server restart.
    ///
    /// - `change`: (`String`, `Option<SeqV>`, `Option<SeqV>`)
    ///   - key: user application key
    ///   - old: previous value (`None` if new key)
    ///   - new: new value (`None` if deleted)
    ///
    /// Called for every successful create, update, or delete.
    /// Implementations without subscribers may leave this empty.
    fn on_change_applied(&mut self, change: (String, Option<SeqV>, Option<SeqV>));
}
