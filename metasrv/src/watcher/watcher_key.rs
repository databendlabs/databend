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

use core::cmp::Ordering;
use std::fmt::Debug;

use common_meta_types::protobuf::watch_request::FilterType;

use super::WatcherId;

/// `WatcherKey` is a wrapper of `WatcherId` and `filter`, but compare only use `id`.
#[derive(Debug, Clone, Default, Eq)]
pub struct WatcherKey {
    pub id: WatcherId,
    pub filter: FilterType,
}

impl PartialEq for WatcherKey {
    fn eq(&self, other: &WatcherKey) -> bool {
        self.id == other.id
    }
}

impl Ord for WatcherKey {
    fn cmp(&self, other: &WatcherKey) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for WatcherKey {
    fn partial_cmp(&self, other: &WatcherKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
