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

use std::io;

use crate::map_api_ro::MapApiRO;
use crate::map_key::MapKey;
use crate::Transition;

/// Provide a read-write key-value map API set, used to access state machine data.
#[async_trait::async_trait]
pub trait MapApi<K, M>: MapApiRO<K, M>
where
    K: MapKey<M>,
    M: Unpin,
{
    /// Set an entry and returns the old value and the new value.
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<M>)>,
    ) -> Result<Transition<crate::MarkedOf<K, M>>, io::Error>;
}
