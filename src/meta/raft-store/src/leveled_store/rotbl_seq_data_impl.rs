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

use seq_marked::SeqData;
use state_machine_api::KVMeta;
use state_machine_api::MetaValue;

use crate::leveled_store::value_convert::ValueConvert;

impl ValueConvert<SeqData> for SeqData<MetaValue> {
    fn conv_to(self) -> Result<SeqData, io::Error> {
        let (seq, meta_value) = self.into_parts();

        let bytes = meta_value.conv_to()?;

        let seq_data = SeqData::new(seq, bytes);

        Ok(seq_data)
    }

    fn conv_from(seq_marked: SeqData) -> Result<Self, io::Error> {
        let (seq, data) = seq_marked.into_parts();

        let meta_value = ValueConvert::conv_from(data)?;

        let seq_data = SeqData::new(seq, meta_value);

        Ok(seq_data)
    }
}

/// Conversion for ExpireValue
impl ValueConvert<SeqData> for SeqData<String> {
    fn conv_to(self) -> Result<SeqData, io::Error> {
        let sys_data = self.map(|s| (None::<KVMeta>, s.into_bytes()));

        sys_data.conv_to()
    }

    fn conv_from(seq_data: SeqData) -> Result<Self, io::Error> {
        let marked = SeqData::<(Option<KVMeta>, Vec<u8>)>::conv_from(seq_data)?;

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
    use crate::leveled_store::value_convert::ValueConvert;

    #[test]
    fn test_marked_of_string_try_from_seq_marked() -> io::Result<()> {
        t_string_try_from(
            SeqData::<String>::new(1, s("hello")),
            SeqData::new(1, b("\x01\x04null\x05hello")),
        );

        Ok(())
    }

    fn t_string_try_from(marked: SeqData<String>, seq_marked: SeqData) {
        let got: SeqData = marked.clone().conv_to().unwrap();
        assert_eq!(seq_marked, got);

        let got = SeqData::<String>::conv_from(got).unwrap();
        assert_eq!(marked, got);
    }

    #[test]
    fn test_marked_try_from_seq_marked() -> io::Result<()> {
        t_try_from(
            SeqData::new(1, (None, b("hello"))),
            SeqData::new(1, b("\x01\x04null\x05hello")),
        );

        t_try_from(
            SeqData::new(1, (Some(KVMeta::new_expires_at(20)), b("hello"))),
            SeqData::new(1, b("\x01\x10{\"expire_at\":20}\x05hello")),
        );

        Ok(())
    }

    fn t_try_from(marked: SeqData<MetaValue>, seq_marked: SeqData) {
        let got: SeqData = marked.clone().conv_to().unwrap();
        assert_eq!(seq_marked, got);

        let got = SeqData::conv_from(got).unwrap();
        assert_eq!(marked, got);
    }

    #[test]
    fn test_invalid_seq_marked() {
        t_invalid(
            SeqData::new(1, b("\x00\x10{\"expire_at\":20}\x05hello")),
            "unsupported rotbl value version: 0",
        );

        t_invalid(
            SeqData::new(1, b("\x01\x10{\"expire_at\":2x}\x05hello")),
            "fail to decode KVMeta from rotbl value: expected `,` or `}` at line 1 column 15",
        );

        t_invalid(
            SeqData::new(1, b("\x01\x10{\"expire_at\":20}\x05h")),
            "fail to decode rotbl value: UnexpectedEnd { additional: 4 }",
        );

        t_invalid(
            SeqData::new(1, b("\x01\x10{\"expire_at\":20}\x05hello-")),
            "remaining bytes in rotbl value: has read: 24, total: 25",
        );
    }

    fn t_invalid(seq_mark: SeqData, want_err: impl ToString) {
        let res = SeqData::<(Option<KVMeta>, Vec<u8>)>::conv_from(seq_mark);
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
