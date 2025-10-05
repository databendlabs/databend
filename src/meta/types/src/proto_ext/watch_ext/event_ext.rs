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

use std::fmt;
use std::time::Duration;

use display_more::DisplayOptionExt;

use crate::normalize_meta::NormalizeMeta;
use crate::protobuf as pb;

impl pb::Event {
    pub fn new(key: impl ToString) -> Self {
        pb::Event {
            key: key.to_string(),
            prev: None,
            current: None,
        }
    }

    pub fn with_prev(mut self, prev: pb::SeqV) -> Self {
        self.prev = Some(prev);
        self
    }

    pub fn with_current(mut self, current: pb::SeqV) -> Self {
        self.current = Some(current);
        self
    }

    pub fn close_to(&self, other: &Self, tolerance: Duration) -> bool {
        if self.key != other.key {
            return false;
        }

        match (&self.prev, &other.prev) {
            (Some(prev), Some(other_prev)) => {
                if !prev.close_to(other_prev, tolerance) {
                    return false;
                }
            }
            (None, None) => {}
            _ => return false,
        }

        match (&self.current, &other.current) {
            (Some(current), Some(other_current)) => {
                if !current.close_to(other_current, tolerance) {
                    return false;
                }
            }
            (None, None) => {}
            _ => return false,
        }

        true
    }
}

impl fmt::Display for pb::Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}: {} -> {})",
            self.key,
            self.prev.display(),
            self.current.display()
        )
    }
}

impl NormalizeMeta for pb::Event {
    fn without_proposed_at(mut self) -> Self {
        if let Some(ref mut prev) = self.prev {
            if let Some(ref mut meta) = prev.meta {
                meta.proposed_at_ms = None;
            }
        }
        if let Some(ref mut current) = self.current {
            if let Some(ref mut meta) = current.meta {
                meta.proposed_at_ms = None;
            }
        }
        self.normalize()
    }

    fn normalize(mut self) -> Self {
        if let Some(ref mut prev) = self.prev {
            if prev.meta == Some(Default::default()) {
                prev.meta = None;
            }
        }
        if let Some(ref mut current) = self.current {
            if current.meta == Some(Default::default()) {
                current.meta = None;
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_display() {
        let event = pb::Event {
            key: "test_key".to_string(),
            prev: Some(pb::SeqV {
                seq: 1,
                data: "test_prev".as_bytes().to_vec(),
                meta: Some(pb::KvMeta {
                    expire_at: Some(1723102819),
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
            }),
            current: Some(pb::SeqV {
                seq: 2,
                data: "test_current".as_bytes().to_vec(),
                meta: None,
            }),
        };
        assert_eq!(event.to_string(), "(test_key: (seq=1 [expire=2024-08-08T07:40:19.000, proposed=2024-08-08T07:40:00.000] 'test_prev') -> (seq=2 [] 'test_current'))");
    }

    #[test]
    fn test_event_close_to() {
        // prev.expire diff -1s

        let e1 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102819), None)),
            b"".to_vec(),
        ));
        let e2 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102818), None)),
            b"".to_vec(),
        ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // prev.expire diff +1s

        let e1 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102819), None)),
            b"".to_vec(),
        ));
        let e2 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102820), None)),
            b"".to_vec(),
        ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // prev.expire diff +1000 millis

        let e1 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102819), None)),
            b"".to_vec(),
        ));
        let e2 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1_723_102_820_000), None)),
            b"".to_vec(),
        ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // current.expire diff +1000 millis

        let e1 = pb::Event::new("k1").with_current(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102819), None)),
            b"".to_vec(),
        ));
        let e2 = pb::Event::new("k1").with_current(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1_723_102_820_000), None)),
            b"".to_vec(),
        ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // prev and current.expire diff +1000 millis

        let e1 = pb::Event::new("k1")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new(Some(1823102819), None)),
                b"".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new(Some(1723102819), None)),
                b"".to_vec(),
            ));
        let e2 = pb::Event::new("k1")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new(Some(1823102818), None)),
                b"".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new(Some(1_723_102_820_000), None)),
                b"".to_vec(),
            ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // prev diff 1s current.expire diff +2000 millis

        let e1 = pb::Event::new("k1")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new(Some(1823102819), None)),
                b"".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new(Some(1723102819), None)),
                b"".to_vec(),
            ));
        let e2 = pb::Event::new("k1")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new(Some(1823102818), None)),
                b"".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new(Some(1_723_102_821_000), None)),
                b"".to_vec(),
            ));

        assert!(!e1.close_to(&e2, Duration::from_secs(1)));
    }

    #[test]
    fn test_event_erase_proposed_at() {
        // Test with both prev and current having proposed_at_ms
        let event = pb::Event::new("test_key")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta {
                    expire_at: Some(1723102819),
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
                b"prev".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                2,
                Some(pb::KvMeta {
                    expire_at: None,
                    proposed_at_ms: Some(1_723_102_900_000),
                }),
                b"current".to_vec(),
            ));

        let erased = event.without_proposed_at();

        // Check prev: expire_at should remain, proposed_at_ms should be None
        assert_eq!(erased.prev.as_ref().unwrap().seq, 1);
        let prev_meta = erased.prev.as_ref().unwrap().meta.as_ref().unwrap();
        assert_eq!(prev_meta.expire_at, Some(1723102819));
        assert_eq!(prev_meta.proposed_at_ms, None);

        // Check current: meta should be removed (default after erasing proposed_at_ms)
        assert_eq!(erased.current.as_ref().unwrap().seq, 2);
        assert_eq!(erased.current.as_ref().unwrap().meta, None);
    }

    #[test]
    fn test_event_reduce() {
        // Test reduce without touching proposed_at_ms
        let event = pb::Event::new("test_key")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta {
                    expire_at: None,
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
                b"prev".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                2,
                Some(pb::KvMeta {
                    expire_at: Some(1723102819),
                    proposed_at_ms: None,
                }),
                b"current".to_vec(),
            ));

        let reduced = event.normalize();

        // Check prev: proposed_at_ms should remain, but meta removed (only proposed_at is not default)
        assert_eq!(reduced.prev.as_ref().unwrap().seq, 1);
        let prev_meta = reduced.prev.as_ref().unwrap().meta.as_ref().unwrap();
        assert_eq!(prev_meta.proposed_at_ms, Some(1_723_102_800_000));
        assert_eq!(prev_meta.expire_at, None);

        // Check current: meta should remain (has expire_at)
        assert_eq!(reduced.current.as_ref().unwrap().seq, 2);
        let current_meta = reduced.current.as_ref().unwrap().meta.as_ref().unwrap();
        assert_eq!(current_meta.expire_at, Some(1723102819));
        assert_eq!(current_meta.proposed_at_ms, None);
    }

    #[test]
    fn test_event_reduce_removes_default_meta() {
        // Test that reduce removes metadata when it's default
        let event = pb::Event::new("test_key")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta {
                    expire_at: None,
                    proposed_at_ms: None,
                }),
                b"prev".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                2,
                Some(Default::default()),
                b"current".to_vec(),
            ));

        let reduced = event.normalize();

        // Both prev and current should have meta removed
        assert_eq!(reduced.prev.as_ref().unwrap().meta, None);
        assert_eq!(reduced.current.as_ref().unwrap().meta, None);
    }

    #[test]
    fn test_event_erase_proposed_at_then_reduce() {
        // Test the chain: erase_proposed_at calls reduce
        let event = pb::Event::new("test_key").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta {
                expire_at: None,
                proposed_at_ms: Some(1_723_102_800_000),
            }),
            b"prev".to_vec(),
        ));

        let erased = event.without_proposed_at();

        // Meta should be removed entirely (was only proposed_at_ms, now default)
        assert_eq!(erased.prev.as_ref().unwrap().meta, None);
    }
}
