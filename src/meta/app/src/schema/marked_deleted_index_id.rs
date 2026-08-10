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

use databend_meta_client::kvapi;

#[derive(Clone, Debug, Copy, Eq, PartialEq, PartialOrd, Ord, kvapi::KeyCodec)]
pub struct MarkedDeletedIndexId {
    pub table_id: u64,
    pub index_id: u64,
}

impl MarkedDeletedIndexId {
    pub fn new(table_id: u64, index_id: u64) -> Self {
        MarkedDeletedIndexId { table_id, index_id }
    }
}
