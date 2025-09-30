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

use state_machine_api::SeqV;

use crate::protobuf as pb;
use crate::protobuf::StreamItem;
use crate::reduce_seqv::ReduceSeqV;

impl StreamItem {
    pub fn new(key: String, value: Option<pb::SeqV>) -> Self {
        StreamItem { key, value }
    }

    pub fn into_option_pair(self) -> (String, Option<SeqV>) {
        (self.key, self.value.map(SeqV::from))
    }

    pub fn into_pair(self) -> (String, SeqV) {
        (self.key, SeqV::from(self.value.unwrap()))
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

impl ReduceSeqV for pb::StreamItem {
    fn erase_proposed_at(mut self) -> Self {
        if let Some(ref mut value) = self.value {
            if let Some(ref mut meta) = value.meta {
                meta.proposed_at_ms = None;
            }
        }
        self.reduce()
    }

    fn reduce(mut self) -> Self {
        if let Some(ref mut value) = self.value {
            if value.meta == Some(Default::default()) {
                value.meta = None;
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_item_erase_proposed_at() {
        // Test with value having proposed_at_ms
        let item = pb::StreamItem::new(
            "test_key".to_string(),
            Some(pb::SeqV {
                seq: 1,
                data: b"test_data".to_vec(),
                meta: Some(pb::KvMeta {
                    expire_at: Some(1723102819),
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
            }),
        );

        let erased = item.erase_proposed_at();

        // Check: expire_at should remain, proposed_at_ms should be None
        assert_eq!(erased.key, "test_key");
        let value = erased.value.as_ref().unwrap();
        assert_eq!(value.seq, 1);
        let meta = value.meta.as_ref().unwrap();
        assert_eq!(meta.expire_at, Some(1723102819));
        assert_eq!(meta.proposed_at_ms, None);
    }

    #[test]
    fn test_stream_item_erase_proposed_at_removes_default_meta() {
        // Test that erase_proposed_at removes meta when it becomes default
        let item = pb::StreamItem::new(
            "test_key".to_string(),
            Some(pb::SeqV {
                seq: 2,
                data: b"data".to_vec(),
                meta: Some(pb::KvMeta {
                    expire_at: None,
                    proposed_at_ms: Some(1_723_102_900_000),
                }),
            }),
        );

        let erased = item.erase_proposed_at();

        // Meta should be removed entirely (was only proposed_at_ms, now default)
        assert_eq!(erased.value.as_ref().unwrap().meta, None);
    }

    #[test]
    fn test_stream_item_reduce() {
        // Test reduce without touching proposed_at_ms
        let item = pb::StreamItem::new(
            "test_key".to_string(),
            Some(pb::SeqV {
                seq: 1,
                data: b"test_data".to_vec(),
                meta: Some(pb::KvMeta {
                    expire_at: None,
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
            }),
        );

        let reduced = item.reduce();

        // proposed_at_ms should remain, meta not removed (not default)
        let value = reduced.value.as_ref().unwrap();
        let meta = value.meta.as_ref().unwrap();
        assert_eq!(meta.proposed_at_ms, Some(1_723_102_800_000));
        assert_eq!(meta.expire_at, None);
    }

    #[test]
    fn test_stream_item_reduce_removes_default_meta() {
        // Test that reduce removes metadata when it's default
        let item = pb::StreamItem::new(
            "test_key".to_string(),
            Some(pb::SeqV {
                seq: 3,
                data: b"data".to_vec(),
                meta: Some(pb::KvMeta {
                    expire_at: None,
                    proposed_at_ms: None,
                }),
            }),
        );

        let reduced = item.reduce();

        // Meta should be removed (is default)
        assert_eq!(reduced.value.as_ref().unwrap().meta, None);
    }

    #[test]
    fn test_stream_item_reduce_keeps_non_default_meta() {
        // Test that reduce keeps metadata when it has expire_at
        let item = pb::StreamItem::new(
            "test_key".to_string(),
            Some(pb::SeqV {
                seq: 1,
                data: b"test_data".to_vec(),
                meta: Some(pb::KvMeta {
                    expire_at: Some(1723102819),
                    proposed_at_ms: None,
                }),
            }),
        );

        let reduced = item.reduce();

        // Meta should remain (has expire_at)
        let value = reduced.value.as_ref().unwrap();
        let meta = value.meta.as_ref().unwrap();
        assert_eq!(meta.expire_at, Some(1723102819));
        assert_eq!(meta.proposed_at_ms, None);
    }

    #[test]
    fn test_stream_item_with_no_value() {
        // Test with None value
        let item = pb::StreamItem::new("key".to_string(), None);

        let erased = item.clone().erase_proposed_at();
        let reduced = item.reduce();

        assert_eq!(erased.value, None);
        assert_eq!(reduced.value, None);
    }

    #[test]
    fn test_stream_item_with_no_meta() {
        // Test with value but no meta
        let item = pb::StreamItem::new(
            "test_key".to_string(),
            Some(pb::SeqV {
                seq: 1,
                data: b"data".to_vec(),
                meta: None,
            }),
        );

        let erased = item.clone().erase_proposed_at();
        let reduced = item.reduce();

        assert_eq!(erased.value.as_ref().unwrap().meta, None);
        assert_eq!(reduced.value.as_ref().unwrap().meta, None);
    }
}
