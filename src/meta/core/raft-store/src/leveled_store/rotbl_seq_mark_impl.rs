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

//! Implement the conversion between `rotbl::SeqMarked` and `Marked` that is used by this crate.
//!
//! `UserKey`
//! `SeqV <-> SeqMarked<(Option<KVMeta>, bytes)> <-> SeqMarked`
//!
//! `ExpireKey`
//! `ExpireValue <-> SeqMarked<String> <-> SeqMarked`

use std::io;

use rotbl::v001::Marked;
use rotbl::v001::SeqMarked;
use state_machine_api::KVMeta;
use state_machine_api::MetaValue;

use crate::leveled_store::persisted_codec::PersistedCodec;

impl PersistedCodec<SeqMarked> for SeqMarked<MetaValue> {
    fn encode_to(self) -> Result<SeqMarked, io::Error> {
        let (seq, data) = self.into_parts();

        let seq_marked = match data {
            Marked::TombStone => SeqMarked::new_tombstone(seq),
            Marked::Normal(meta_value) => {
                let bytes = meta_value.encode_to()?;

                SeqMarked::new_normal(seq, bytes)
            }
        };
        Ok(seq_marked)
    }

    fn decode_from(seq_marked: SeqMarked) -> Result<Self, io::Error> {
        let (seq, data) = seq_marked.into_parts();

        let data = match data {
            Marked::TombStone => {
                return Ok(SeqMarked::new_tombstone(seq));
            }
            Marked::Normal(bytes) => bytes,
        };

        let meta_value = PersistedCodec::decode_from(data)?;

        let marked = SeqMarked::new_normal(seq, meta_value);
        Ok(marked)
    }
}

/// Conversion for ExpireValue
impl PersistedCodec<SeqMarked> for SeqMarked<String> {
    fn encode_to(self) -> Result<SeqMarked, io::Error> {
        let marked = self.map(|s| (None::<KVMeta>, s.into_bytes()));

        marked.encode_to()
    }

    fn decode_from(seq_marked: SeqMarked) -> Result<Self, io::Error> {
        let marked = SeqMarked::<(Option<KVMeta>, Vec<u8>)>::decode_from(seq_marked)?;
        let marked = marked.try_map(|(_meta, value)| {
            String::from_utf8(value).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("fail to decode String from bytes: {}", e),
                )
            })
        })?;

        Ok(marked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leveled_store::persisted_codec::PersistedCodec;

    #[test]
    fn test_marked_of_string_try_from_seq_marked() -> io::Result<()> {
        t_string_try_from(
            SeqMarked::<String>::new_normal(1, s("hello")),
            SeqMarked::new_normal(1, b("\x01\x04null\x05hello")),
        );

        t_string_try_from(
            SeqMarked::<String>::new_tombstone(2),
            SeqMarked::new_tombstone(2),
        );
        Ok(())
    }

    fn t_string_try_from(marked: SeqMarked<String>, seq_marked: SeqMarked) {
        let got: SeqMarked = marked.clone().encode_to().unwrap();
        assert_eq!(seq_marked, got);

        let got = SeqMarked::<String>::decode_from(got).unwrap();
        assert_eq!(marked, got);
    }

    #[test]
    fn test_marked_try_from_seq_marked() -> io::Result<()> {
        t_try_from(
            SeqMarked::new_normal(1, (None, b("hello"))),
            SeqMarked::new_normal(1, b("\x01\x04null\x05hello")),
        );

        t_try_from(
            SeqMarked::new_normal(1, (Some(KVMeta::new_expires_at(20)), b("hello"))),
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":20}\x05hello")),
        );

        t_try_from(SeqMarked::new_tombstone(2), SeqMarked::new_tombstone(2));
        Ok(())
    }

    fn t_try_from(marked: SeqMarked<MetaValue>, seq_marked: SeqMarked) {
        let got: SeqMarked = marked.clone().encode_to().unwrap();
        assert_eq!(seq_marked, got);

        let got = SeqMarked::decode_from(got).unwrap();
        assert_eq!(marked, got);
    }

    #[test]
    fn test_invalid_seq_marked() {
        t_invalid(
            SeqMarked::new_normal(1, b("\x00\x10{\"expire_at\":20}\x05hello")),
            "unsupported rotbl value version: 0",
        );

        t_invalid(
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":2x}\x05hello")),
            "fail to decode KVMeta from rotbl value: expected `,` or `}` at line 1 column 15",
        );

        t_invalid(
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":20}\x05h")),
            "fail to decode rotbl value: UnexpectedEnd { additional: 4 }",
        );

        t_invalid(
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":20}\x05hello-")),
            "remaining bytes in rotbl value: has read: 24, total: 25",
        );
    }

    fn t_invalid(seq_mark: SeqMarked, want_err: impl ToString) {
        let res = SeqMarked::<(Option<KVMeta>, Vec<u8>)>::decode_from(seq_mark);
        let err = res.unwrap_err();
        assert_eq!(want_err.to_string(), err.to_string());
    }

    fn s(v: impl ToString) -> String {
        v.to_string()
    }

    fn b(v: impl ToString) -> Vec<u8> {
        v.to_string().into_bytes()
    }
}
