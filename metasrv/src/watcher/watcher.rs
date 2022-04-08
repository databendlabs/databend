//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_meta_types::protobuf::WatchCreateRequest;

use super::WatcherId;
use super::WatcherStreamId;

#[derive(Debug)]
pub struct Watcher {
    pub id: WatcherId,

    // owner watcher stream id
    pub owner_id: WatcherStreamId,

    pub key: String,

    pub key_end: String,
}

impl Watcher {
    pub fn new(id: WatcherId, owner_id: WatcherStreamId, create: WatchCreateRequest) -> Watcher {
        Watcher {
            id,
            owner_id,
            key: create.key,
            key_end: create.key_end,
        }
    }
}
