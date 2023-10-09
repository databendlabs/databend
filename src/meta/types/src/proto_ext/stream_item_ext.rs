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

use crate::protobuf as pb;
use crate::protobuf::StreamItem;
use crate::SeqV;

impl StreamItem {
    pub fn new(key: String, value: Option<pb::SeqV>) -> Self {
        StreamItem { key, value }
    }
}

impl From<(String, Option<pb::SeqV>)> for StreamItem {
    fn from(kv: (String, Option<pb::SeqV>)) -> Self {
        StreamItem::new(kv.0, kv.1)
    }
}

impl From<(String, Option<SeqV>)> for StreamItem {
    fn from(kv: (String, Option<SeqV>)) -> Self {
        StreamItem::new(kv.0, kv.1.map(pb::SeqV::from))
    }
}

impl From<(String, SeqV)> for StreamItem {
    fn from(kv: (String, SeqV)) -> Self {
        StreamItem::new(kv.0, Some(pb::SeqV::from(kv.1)))
    }
}
