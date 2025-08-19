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
                }),
            }),
            current: Some(pb::SeqV {
                seq: 2,
                data: "test_current".as_bytes().to_vec(),
                meta: None,
            }),
        };
        assert_eq!(event.to_string(), "(test_key: (seq=1 [expire=2024-08-08T07:40:19.000] 'test_prev') -> (seq=2 [] 'test_current'))");
    }

    #[test]
    fn test_event_close_to() {
        // prev.expire diff -1s

        let e1 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new_expire(1723102819)),
            b"".to_vec(),
        ));
        let e2 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new_expire(1723102818)),
            b"".to_vec(),
        ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // prev.expire diff +1s

        let e1 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new_expire(1723102819)),
            b"".to_vec(),
        ));
        let e2 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new_expire(1723102820)),
            b"".to_vec(),
        ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // prev.expire diff +1000 millis

        let e1 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new_expire(1723102819)),
            b"".to_vec(),
        ));
        let e2 = pb::Event::new("k1").with_prev(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new_expire(1_723_102_820_000)),
            b"".to_vec(),
        ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // current.expire diff +1000 millis

        let e1 = pb::Event::new("k1").with_current(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new_expire(1723102819)),
            b"".to_vec(),
        ));
        let e2 = pb::Event::new("k1").with_current(pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new_expire(1_723_102_820_000)),
            b"".to_vec(),
        ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // prev and current.expire diff +1000 millis

        let e1 = pb::Event::new("k1")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new_expire(1823102819)),
                b"".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new_expire(1723102819)),
                b"".to_vec(),
            ));
        let e2 = pb::Event::new("k1")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new_expire(1823102818)),
                b"".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new_expire(1_723_102_820_000)),
                b"".to_vec(),
            ));

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        // prev diff 1s current.expire diff +2000 millis

        let e1 = pb::Event::new("k1")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new_expire(1823102819)),
                b"".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new_expire(1723102819)),
                b"".to_vec(),
            ));
        let e2 = pb::Event::new("k1")
            .with_prev(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new_expire(1823102818)),
                b"".to_vec(),
            ))
            .with_current(pb::SeqV::with_meta(
                1,
                Some(pb::KvMeta::new_expire(1_723_102_821_000)),
                b"".to_vec(),
            ));

        assert!(!e1.close_to(&e2, Duration::from_secs(1)));
    }
}
