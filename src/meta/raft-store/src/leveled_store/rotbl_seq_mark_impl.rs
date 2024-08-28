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

use std::io;

use databend_common_meta_types::seq_value::KVMeta;
use rotbl::v001::SeqMarked;

use crate::marked::Marked;

impl TryFrom<SeqMarked> for Marked {
    type Error = io::Error;

    /// Convert `rotbl::SeqMarked` to `Marked` that is used by this crate.
    fn try_from(seq_marked: SeqMarked) -> Result<Self, Self::Error> {
        let seq = seq_marked.seq();

        let Some(data) = seq_marked.into_data() else {
            return Ok(Marked::new_tombstone(seq));
        };

        // version, meta, value
        let ((ver, meta_str, value), size): ((u8, String, Vec<u8>), usize) =
            bincode::decode_from_slice(&data, bincode_config()).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("fail to decode rotbl::SeqMarked value: {}", e),
                )
            })?;

        if ver != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported rotbl::SeqMarked version: {}", ver),
            ));
        }

        if size != data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "remaining bytes in rotbl::SeqMarked: has read: {}, total: {}",
                    size,
                    data.len()
                ),
            ));
        }

        let kv_meta: Option<KVMeta> = serde_json::from_str(&meta_str).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("fail to decode KVMeta from rotbl::SeqMarked: {}", e),
            )
        })?;

        let marked = Marked::new_with_meta(seq, value, kv_meta);
        Ok(marked)
    }
}

impl TryFrom<SeqMarked> for Marked<String> {
    type Error = io::Error;

    /// Try converting `rotbl::SeqMarked` to `Marked<String>`.
    fn try_from(value: SeqMarked) -> Result<Self, Self::Error> {
        let marked = Marked::try_from(value)?;
        match marked {
            Marked::TombStone { internal_seq } => Ok(Marked::TombStone { internal_seq }),
            Marked::Normal {
                internal_seq,
                value,
                meta,
            } => Ok(Marked::Normal {
                internal_seq,
                value: String::from_utf8(value).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("fail to convert Vec<u8> to String: {}", e),
                    )
                })?,
                meta,
            }),
        }
    }
}

impl TryFrom<Marked> for SeqMarked {
    type Error = io::Error;

    fn try_from(marked: Marked) -> Result<Self, Self::Error> {
        // internal_seq() is the storage seq of the record.
        // seq() returns seq for user, which 0 for a tombstone.
        let seq_marked = match marked {
            Marked::TombStone { internal_seq } => SeqMarked::new_tombstone(internal_seq),
            Marked::Normal {
                internal_seq,
                value,
                meta,
            } => {
                let kv_meta_str = serde_json::to_string(&meta).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("fail to encode KVMeta to json: {}", e),
                    )
                })?;

                // version, meta in json string, value
                let packed = (1u8, kv_meta_str, value);

                let d = bincode::encode_to_vec(packed, bincode_config()).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("fail to encode rotbl::SeqMarked value: {}", e),
                    )
                })?;

                SeqMarked::new_normal(internal_seq, d)
            }
        };
        Ok(seq_marked)
    }
}

impl TryFrom<Marked<String>> for SeqMarked {
    type Error = io::Error;

    fn try_from(value: Marked<String>) -> Result<Self, Self::Error> {
        let marked = Marked::<Vec<u8>>::from(value);
        SeqMarked::try_from(marked)
    }
}

fn bincode_config() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_big_endian()
        .with_variable_int_encoding()
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn test_marked_of_string_try_from_seq_marked() -> io::Result<()> {
        t_string_try_from(
            Marked::<String>::new_with_meta(1, s("hello"), None),
            SeqMarked::new_normal(1, b("\x01\x04null\x05hello")),
        );

        t_string_try_from(
            Marked::<String>::new_with_meta(1, s("hello"), Some(KVMeta::new_expire(20))),
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":20}\x05hello")),
        );

        t_string_try_from(
            Marked::<String>::new_tombstone(2),
            SeqMarked::new_tombstone(2),
        );
        Ok(())
    }

    fn t_string_try_from(marked: Marked<String>, seq_marked: SeqMarked) {
        let got: SeqMarked = marked.clone().try_into().unwrap();
        assert_eq!(seq_marked, got);

        let got: Marked<String> = got.try_into().unwrap();
        assert_eq!(marked, got);
    }

    #[test]
    fn test_marked_try_from_seq_marked() -> io::Result<()> {
        t_try_from(
            Marked::new_with_meta(1, b("hello"), None),
            SeqMarked::new_normal(1, b("\x01\x04null\x05hello")),
        );

        t_try_from(
            Marked::new_with_meta(1, b("hello"), Some(KVMeta::new_expire(20))),
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":20}\x05hello")),
        );

        t_try_from(
            Marked::<Vec<u8>>::new_tombstone(2),
            SeqMarked::new_tombstone(2),
        );
        Ok(())
    }

    fn t_try_from(marked: Marked, seq_marked: SeqMarked) {
        let got: SeqMarked = marked.clone().try_into().unwrap();
        assert_eq!(seq_marked, got);

        let got: Marked = got.try_into().unwrap();
        assert_eq!(marked, got);
    }

    #[test]
    fn test_invalid_seq_marked() {
        t_invalid(
            SeqMarked::new_normal(1, b("\x00\x10{\"expire_at\":20}\x05hello")),
            "unsupported rotbl::SeqMarked version: 0",
        );

        t_invalid(
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":2x}\x05hello")),
            "fail to decode KVMeta from rotbl::SeqMarked: expected `,` or `}` at line 1 column 15",
        );

        t_invalid(
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":20}\x05h")),
            "fail to decode rotbl::SeqMarked value: UnexpectedEnd { additional: 4 }",
        );

        t_invalid(
            SeqMarked::new_normal(1, b("\x01\x10{\"expire_at\":20}\x05hello-")),
            "remaining bytes in rotbl::SeqMarked: has read: 24, total: 25",
        );
    }

    fn t_invalid(seq_mark: SeqMarked, want_err: impl ToString) {
        let res = Marked::<Vec<u8>>::try_from(seq_mark);
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
