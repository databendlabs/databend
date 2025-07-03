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

use display_more::DisplayUnixTimeStampExt;

use crate::flexible_timestamp_to_duration;
use crate::protobuf::KvMeta;
use crate::protobuf::SeqV;

impl fmt::Display for KvMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.expire_at {
            None => {
                write!(f, "[]")
            }
            Some(e) => {
                write!(
                    f,
                    "[expire={}]",
                    flexible_timestamp_to_duration(e).display_unix_timestamp_short()
                )
            }
        }
    }
}

impl fmt::Display for SeqV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(seq={}", self.seq,)?;

        if let Some(m) = &self.meta {
            write!(f, " {m}")?;
        } else {
            write!(f, " []")?;
        }

        if let Ok(x) = std::str::from_utf8(&self.data) {
            write!(f, " '{}')", x,)
        } else {
            write!(f, " {:?})", &self.data,)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protobuf::KvMeta;
    use crate::protobuf::SeqV;

    #[test]
    fn test_kv_meta_display() {
        let meta = KvMeta::default();
        assert_eq!(meta.to_string(), "[]");

        let meta = KvMeta {
            expire_at: Some(1723102819),
        };
        assert_eq!(meta.to_string(), "[expire=2024-08-08T07:40:19.000]");

        let meta = KvMeta {
            expire_at: Some(1723102819_000),
        };
        assert_eq!(meta.to_string(), "[expire=2024-08-08T07:40:19.000]");
    }

    #[test]
    fn test_seqv_display() {
        let seqv = SeqV {
            seq: 1,
            meta: Some(KvMeta::default()),
            data: vec![],
        };
        assert_eq!(seqv.to_string(), "(seq=1 [] '')");

        let seqv = SeqV {
            seq: 1,
            meta: Some(KvMeta {
                expire_at: Some(1723102819),
            }),
            data: vec![65, 66, 67],
        };
        assert_eq!(
            seqv.to_string(),
            "(seq=1 [expire=2024-08-08T07:40:19.000] 'ABC')"
        );

        let seqv = SeqV {
            seq: 1,
            meta: Some(KvMeta {
                expire_at: Some(1723102819_000),
            }),
            data: vec![65, 66, 67],
        };
        assert_eq!(
            seqv.to_string(),
            "(seq=1 [expire=2024-08-08T07:40:19.000] 'ABC')"
        );

        let seqv = SeqV {
            seq: 1,
            meta: Some(KvMeta {
                expire_at: Some(1723102819),
            }),
            data: vec![0, 159, 146, 150],
        };
        assert_eq!(
            seqv.to_string(),
            "(seq=1 [expire=2024-08-08T07:40:19.000] [0, 159, 146, 150])"
        );

        let seqv = SeqV {
            seq: 1,
            meta: Some(KvMeta {
                // in millis
                expire_at: Some(1723102819_000),
            }),
            data: vec![0, 159, 146, 150],
        };
        assert_eq!(
            seqv.to_string(),
            "(seq=1 [expire=2024-08-08T07:40:19.000] [0, 159, 146, 150])"
        );
    }
}
