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

use crate::kvapi;
use crate::kvapi::Key;
use crate::kvapi::KeyCodec;
use crate::kvapi::KeyError;

/// The dir name of a key.
///
/// For example, the dir name of a key `a/b/c` is `a/b`.
///
/// Note that the dir name of `a` is still `a`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirName<K> {
    key: K,
    level: usize,
}

impl<K> DirName<K> {
    pub fn new(key: K) -> Self {
        DirName { key, level: 1 }
    }

    pub fn new_with_level(key: K, level: usize) -> Self {
        DirName { key, level }
    }

    pub fn with_level(&mut self, level: usize) -> &mut Self {
        self.level = level;
        self
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn into_key(self) -> K {
        self.key
    }
}

impl<K> DirName<K>
where K: kvapi::Key
{
    /// Return a string with a suffix slash "/"
    pub fn dir_name_with_slash(&self) -> String {
        let prefix = self.to_string_key();
        format!("{}/", prefix)
    }
}

impl<K: Key> KeyCodec for DirName<K> {
    /// DirName can not be encoded as a key directly
    fn encode_key(&self, _b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
        unimplemented!()
    }

    /// DirName can not be encoded as a key directly
    fn decode_key(_p: &mut kvapi::KeyParser) -> Result<Self, KeyError> {
        unimplemented!()
    }
}

impl<K: Key> Key for DirName<K> {
    const PREFIX: &'static str = K::PREFIX;
    type ValueType = K::ValueType;

    fn parent(&self) -> Option<String> {
        unimplemented!("DirName is not a record thus it has no parent")
    }

    fn to_string_key(&self) -> String {
        let k = self.key.to_string_key();
        k.rsplitn(self.level + 1, '/').last().unwrap().to_string()
    }

    fn from_str_key(s: &str) -> Result<Self, KeyError> {
        let k = K::from_str_key(s)?;
        let d = DirName::new_with_level(k, 0);
        Ok(d)
    }
}

#[cfg(test)]
mod tests {

    use crate::kvapi::dir_name::DirName;
    use crate::kvapi::testing::FooKey;
    use crate::kvapi::Key;

    #[test]
    fn test_dir_name_from_key() {
        let d = DirName::<FooKey>::from_str_key("pref/9/x/8").unwrap();
        assert_eq!(
            FooKey {
                a: 9,
                b: "x".to_string(),
                c: 8,
            },
            d.into_key()
        );
    }

    #[test]
    fn test_dir_name() {
        let k = FooKey {
            a: 1,
            b: "b".to_string(),
            c: 2,
        };

        let dir = DirName::new(k);
        assert_eq!("pref/1/b", dir.to_string_key());

        let dir = DirName::new(dir);
        assert_eq!("pref/1", dir.to_string_key());

        let dir = DirName::new(dir);
        assert_eq!("pref", dir.to_string_key());

        let dir = DirName::new(dir);
        assert_eq!("pref", dir.to_string_key(), "root dir should be the same");
    }

    #[test]
    fn test_dir_name_with_level() {
        let k = FooKey {
            a: 1,
            b: "b".to_string(),
            c: 2,
        };

        let mut dir = DirName::new(k);
        assert_eq!("pref/1/b", dir.to_string_key());

        dir.with_level(0);
        assert_eq!("pref/1/b/2", dir.to_string_key());

        dir.with_level(2);
        assert_eq!("pref/1", dir.to_string_key());

        dir.with_level(3);
        assert_eq!("pref", dir.to_string_key());

        dir.with_level(4);
        assert_eq!("pref", dir.to_string_key(), "root dir should be the same");
    }
}
