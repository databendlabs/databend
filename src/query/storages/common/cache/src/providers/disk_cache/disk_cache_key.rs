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

use std::fmt::Display;
use std::hash::Hasher;
use std::path::Path;
use std::path::PathBuf;

use siphasher::sip128;
use siphasher::sip128::Hasher128;

#[derive(Debug)]
pub struct DiskCacheKey(pub String);

impl<S> From<S> for DiskCacheKey
where S: AsRef<str>
{
    // convert key string into hex string of SipHash 2-4 128 bit
    fn from(key: S) -> Self {
        let mut sip = sip128::SipHasher24::new();
        let key = key.as_ref();
        sip.write(key.as_bytes());
        let hash = sip.finish128();
        let hex_hash = hex::encode(hash.as_bytes());
        DiskCacheKey(hex_hash)
    }
}

impl From<&DiskCacheKey> for PathBuf {
    fn from(cache_key: &DiskCacheKey) -> Self {
        let prefix = &cache_key.0[0..3];
        let mut path_buf = PathBuf::from(prefix);
        path_buf.push(Path::new(&cache_key.0));
        path_buf
    }
}

impl Display for DiskCacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.clone())
    }
}
