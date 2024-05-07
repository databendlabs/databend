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

//! implement the sub key space for the state machine.

use std::fmt;
use std::io::Error;

use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::state_machine::ExpireKey;

impl MapKeyEncode for String {
    const PREFIX: &'static str = "kv--";

    fn encode<W: fmt::Write>(&self, mut w: W) -> Result<(), fmt::Error> {
        w.write_str(self.as_str())
    }
}

impl MapKey for String {
    type V = Vec<u8>;

    fn decode(buf: &str) -> Result<Self, Error> {
        Ok(buf.to_string())
    }
}

impl MapKeyEncode for str {
    const PREFIX: &'static str = "kv--";

    fn encode<W: fmt::Write>(&self, mut w: W) -> Result<(), fmt::Error> {
        w.write_str(self)
    }
}

impl MapKeyEncode for ExpireKey {
    const PREFIX: &'static str = "exp-";

    fn encode<W: fmt::Write>(&self, mut w: W) -> Result<(), fmt::Error> {
        // max u64 len is 20: 18446744073709551616
        w.write_fmt(format_args!("{:>020}/{:>020}", self.time_ms, self.seq))?;
        Ok(())
    }
}

impl MapKey for ExpireKey {
    type V = String;

    fn decode(buf: &str) -> Result<Self, Error> {
        if buf.len() != 41 {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!("ExpireKey len must be 41, but {}: {:?}", buf.len(), buf),
            ));
        }

        let mut segments = buf.split('/');

        let time_ms = segments.next().ok_or_else(|| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("first segment `time_ms` not found: {:?}", buf),
            )
        })?;

        let seq = segments.next().ok_or_else(|| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("second segment `seq` not found: {:?}", buf),
            )
        })?;

        if segments.next().is_some() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!("ExpireKey must have only one '/': {:?}", buf),
            ));
        }

        if time_ms.len() != 20 {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!("`time_ms` len must be 20, but {}: {:?}", time_ms.len(), buf),
            ));
        }

        if seq.len() != 20 {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!("`seq` len must be 20, but {}: {:?}", seq.len(), buf),
            ));
        }

        let time_ms = time_ms.parse().map_err(|_| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("parse `time_ms` failed: {:?}", buf),
            )
        })?;

        let seq = seq.parse().map_err(|_| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("parse `seq` failed: {:?}", buf),
            )
        })?;

        Ok(Self::new(time_ms, seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_encode() {
        let key = "key";
        let mut buf: String = String::new();
        key.encode(&mut buf).unwrap();
        assert_eq!(key, buf);
    }

    #[test]
    fn test_string_encode_decode() {
        let key = "key".to_string();
        let mut buf: String = String::new();
        key.encode(&mut buf).unwrap();
        assert_eq!(key, buf);

        assert_eq!(key, String::decode(&buf).unwrap());
    }

    #[test]
    fn test_expire_encode_decode() {
        #[allow(clippy::redundant_closure)]
        let exp = |time_ms, seq| ExpireKey::new(time_ms, seq);

        t_expire_codec(exp(0, 0), "00000000000000000000/00000000000000000000");
        t_expire_codec(exp(123, 456), "00000000000000000123/00000000000000000456");
        t_expire_codec(
            exp(u64::MAX, u64::MAX),
            "18446744073709551615/18446744073709551615",
        );
    }

    #[test]
    fn test_expire_decode_error() {
        let dec_err = |s| ExpireKey::decode(s).unwrap_err().to_string();

        assert_eq!(r#"ExpireKey len must be 41, but 0: """#, dec_err(""));
        assert_eq!(r#"ExpireKey len must be 41, but 3: "foo""#, dec_err("foo"));
        assert_eq!(
            r#"ExpireKey must have only one '/': "00000000000000000123/000000000000000001/2""#,
            dec_err("00000000000000000123/000000000000000001/2")
        );
        assert_eq!(
            r#"`time_ms` len must be 20, but 19: "0000000000000000123/000000000000000000456""#,
            dec_err("0000000000000000123/000000000000000000456")
        );
        assert_eq!(
            r#"parse `time_ms` failed: "0000000000000000012a/00000000000000000456""#,
            dec_err("0000000000000000012a/00000000000000000456")
        );
        assert_eq!(
            r#"parse `seq` failed: "00000000000000000123/0000000000000000045a""#,
            dec_err("00000000000000000123/0000000000000000045a")
        );
    }

    #[test]
    fn test_append_to_existing_str() {
        let mut buf = String::with_capacity(64);
        buf.push('e');
        buf.push('/');

        let expire = ExpireKey::new(123, 456);
        expire.encode(&mut buf).unwrap();
        assert_eq!("e/00000000000000000123/00000000000000000456", buf);

        let got = ExpireKey::decode(&buf[2..]).unwrap();
        assert_eq!(expire, got);
    }

    fn t_expire_codec(key: ExpireKey, want: &str) {
        let mut buf = String::new();
        key.encode(&mut buf).unwrap();
        assert_eq!(
            want, buf,
            "encoding: want: {}, got: {}, key: {:?}",
            want, buf, key
        );

        let got = ExpireKey::decode(&buf).unwrap();
        assert_eq!(
            key, got,
            "decoding: want: {}, got: {}, key: {:?}",
            key, got, key
        );
    }
}
