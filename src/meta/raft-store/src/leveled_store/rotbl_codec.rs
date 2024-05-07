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

//! Implement codec between [`MapKey`] and rotbl key in string.

use std::fmt;
use std::fmt::Write;
use std::io;
use std::ops::Bound;
use std::ops::RangeBounds;

use rotbl::v001::SeqMarked;

use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::marked::Marked;

pub struct RotblCodec;

impl RotblCodec {
    /// Convert range of user key to range of rotbl range.
    pub(crate) fn encode_range<K, R>(range: &R) -> Result<(Bound<String>, Bound<String>), io::Error>
    where
        K: MapKey,
        R: RangeBounds<K>,
    {
        let s = range.start_bound();
        let e = range.end_bound();

        let s = Self::encode_bound(s, "left")?;
        let e = Self::encode_bound(e, "right")?;

        Ok((s, e))
    }

    /// Convert user key range bound to rotbl key bound.
    ///
    /// `MapKey::PREFIX` is prepended to the bound value and an open bound is converted to a bound with key-space.
    /// E.g., use the `PREFIX/` as the left side closed bound,
    /// and use the `next(PREFIX/)` as the right side open bound.
    fn encode_bound<K: MapKey>(v: Bound<&K>, dir: &str) -> Result<Bound<String>, io::Error> {
        let res = match v {
            Bound::Included(k) => Bound::Included(Self::encode_key(k)?),
            Bound::Excluded(k) => Bound::Excluded(Self::encode_key(k)?),
            Bound::Unbounded => {
                if dir == "left" {
                    Bound::Included(format!("{}/", K::PREFIX))
                } else {
                    // `0` is the next char after `/` in ascii table.
                    Bound::Excluded(format!("{}0", K::PREFIX))
                }
            }
        };
        Ok(res)
    }

    /// Encode a key value pair of type `(K: MapKey, V: MapKey::V)` to rotbl key-value: `(String, SeqMarked)`.
    pub(crate) fn encode_key_seq_marked<K>(
        key: &K,
        marked: Marked<K::V>,
    ) -> Result<(String, SeqMarked), io::Error>
    where
        K: MapKey,
        SeqMarked: TryFrom<Marked<K::V>, Error = io::Error>,
    {
        let k = Self::encode_key(key)?;
        let v = SeqMarked::try_from(marked)?;
        Ok((k, v))
    }

    /// Encode a [`MapKey`] implementor to a string with key-space prefix.
    pub(crate) fn encode_key<K>(key: &K) -> Result<String, io::Error>
    where K: MapKeyEncode + ?Sized {
        let mut buf = String::with_capacity(64);
        let x: Result<(), fmt::Error> = try {
            buf.write_str(K::PREFIX)?;
            buf.write_char('/')?;
            key.encode(&mut buf)?;
        };
        x.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(buf)
    }

    /// Decode a key from a string with key-space prefix into [`MapKey`] implementation.
    pub(crate) fn decode_key<K>(str_key: &str) -> Result<K, io::Error>
    where K: MapKey {
        // strip prefix
        let prefix_stripped = str_key.strip_prefix(K::PREFIX).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expect prefix: {}, key: {:?}", K::PREFIX, str_key),
            )
        })?;

        let key = prefix_stripped.strip_prefix('/').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expect / after prefix: {}, key: {:?}", K::PREFIX, str_key),
            )
        })?;

        let key = K::decode(key)?;
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use Bound::Excluded;
    use Bound::Included;

    use super::*;
    use crate::state_machine::ExpireKey;

    #[test]
    fn test_encode_key_string() {
        let r = RotblCodec::encode_key::<String>(&s("a")).unwrap();
        assert_eq!(r, "kv--/a");
    }

    #[test]
    fn test_encode_range_string() {
        let r = RotblCodec::encode_range::<String, _>(&(s("a")..s("z"))).unwrap();
        assert_eq!(r, (Included(s("kv--/a")), Excluded(s("kv--/z"))));

        let r = RotblCodec::encode_range::<String, _>(&(s("a")..=s("z"))).unwrap();
        assert_eq!(r, (Included(s("kv--/a")), Included(s("kv--/z"))));

        let r = RotblCodec::encode_range::<String, _>(&(..s("z"))).unwrap();
        assert_eq!(r, (Included(s("kv--/")), Excluded(s("kv--/z"))));

        let r = RotblCodec::encode_range::<String, _>(&(s("a")..)).unwrap();
        assert_eq!(r, (Included(s("kv--/a")), Excluded(s("kv--0"))));

        let r = RotblCodec::encode_range::<String, _>(&(..)).unwrap();
        assert_eq!(r, (Included(s("kv--/")), Excluded(s("kv--0"))));
    }

    #[test]
    fn test_encode_key_expire() {
        let r = RotblCodec::encode_key::<ExpireKey>(&ExpireKey::new(12, 34)).unwrap();
        assert_eq!(r, "exp-/00000000000000000012/00000000000000000034");
    }

    #[test]
    fn test_encode_range_expire() {
        let r = RotblCodec::encode_range::<ExpireKey, _>(
            &(ExpireKey::new(12, 34)..ExpireKey::new(56, 78)),
        )
        .unwrap();
        assert_eq!(
            r,
            (
                Included("exp-/00000000000000000012/00000000000000000034".to_string()),
                Excluded("exp-/00000000000000000056/00000000000000000078".to_string())
            )
        );

        let r = RotblCodec::encode_range::<ExpireKey, _>(
            &(ExpireKey::new(12, 34)..=ExpireKey::new(56, 78)),
        )
        .unwrap();
        assert_eq!(
            r,
            (
                Included("exp-/00000000000000000012/00000000000000000034".to_string()),
                Included("exp-/00000000000000000056/00000000000000000078".to_string())
            )
        );

        let r = RotblCodec::encode_range::<ExpireKey, _>(&(..ExpireKey::new(56, 78))).unwrap();
        assert_eq!(
            r,
            (
                Included("exp-/".to_string()),
                Excluded("exp-/00000000000000000056/00000000000000000078".to_string())
            )
        );

        let r = RotblCodec::encode_range::<ExpireKey, _>(&(ExpireKey::new(12, 34)..)).unwrap();
        assert_eq!(
            r,
            (
                Included("exp-/00000000000000000012/00000000000000000034".to_string()),
                Excluded("exp-0".to_string())
            )
        );

        let r = RotblCodec::encode_range::<ExpireKey, _>(&(..)).unwrap();
        assert_eq!(
            r,
            (Included("exp-/".to_string()), Excluded("exp-0".to_string()))
        );
    }

    fn s(s: impl ToString) -> String {
        s.to_string()
    }
}
