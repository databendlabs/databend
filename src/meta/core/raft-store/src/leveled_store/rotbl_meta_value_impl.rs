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

use state_machine_api::KVMeta;
use state_machine_api::MetaValue;

use crate::leveled_store::persisted_codec::PersistedCodec;

impl PersistedCodec<Vec<u8>> for MetaValue {
    fn encode_to(self) -> Result<Vec<u8>, io::Error> {
        let (meta, value) = self;

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
                format!("fail to encode rotbl value: {}", e),
            )
        })?;

        Ok(d)
    }

    fn decode_from(data: Vec<u8>) -> Result<Self, io::Error> {
        // version, meta, value
        let ((ver, meta_str, value), size): ((u8, String, Vec<u8>), usize) =
            bincode::decode_from_slice(&data, bincode_config()).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("fail to decode rotbl value: {}", e),
                )
            })?;

        if ver != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported rotbl value version: {}", ver),
            ));
        }

        if size != data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "remaining bytes in rotbl value: has read: {}, total: {}",
                    size,
                    data.len()
                ),
            ));
        }

        let kv_meta: Option<KVMeta> = serde_json::from_str(&meta_str).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("fail to decode KVMeta from rotbl value: {}", e),
            )
        })?;

        let meta_value = (kv_meta, value);
        Ok(meta_value)
    }
}

fn bincode_config() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_big_endian()
        .with_variable_int_encoding()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leveled_store::persisted_codec::PersistedCodec;

    #[test]
    fn test_meta_value_try_from_bytes() -> io::Result<()> {
        t_try_from((None, b("hello")), b("\x01\x04null\x05hello"));

        t_try_from(
            (Some(KVMeta::new_expires_at(20)), b("hello")),
            b("\x01\x10{\"expire_at\":20}\x05hello"),
        );

        Ok(())
    }

    fn t_try_from(marked: MetaValue, seq_marked: Vec<u8>) {
        let got: Vec<u8> = marked.clone().encode_to().unwrap();
        assert_eq!(seq_marked, got);

        let got = MetaValue::decode_from(got).unwrap();
        assert_eq!(marked, got);
    }

    #[test]
    fn test_invalid_data() {
        t_invalid(
            b("\x00\x10{\"expire_at\":20}\x05hello"),
            "unsupported rotbl value version: 0",
        );

        t_invalid(
            b("\x01\x10{\"expire_at\":2x}\x05hello"),
            "fail to decode KVMeta from rotbl value: expected `,` or `}` at line 1 column 15",
        );

        t_invalid(
            b("\x01\x10{\"expire_at\":20}\x05h"),
            "fail to decode rotbl value: UnexpectedEnd { additional: 4 }",
        );

        t_invalid(
            b("\x01\x10{\"expire_at\":20}\x05hello-"),
            "remaining bytes in rotbl value: has read: 24, total: 25",
        );
    }

    fn t_invalid(data: Vec<u8>, want_err: impl ToString) {
        let res = MetaValue::decode_from(data);
        let err = res.unwrap_err();
        assert_eq!(want_err.to_string(), err.to_string());
    }

    fn b(v: impl ToString) -> Vec<u8> {
        v.to_string().into_bytes()
    }
}
