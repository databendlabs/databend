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

//! A helper for building a string key from a structured key

use crate::kvapi::helper::escape;
use crate::kvapi::helper::escape_specified;

pub struct KeyBuilder {
    buf: Vec<u8>,
}

impl KeyBuilder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn new_prefixed(prefix: &str) -> Self {
        let b = Self::new();
        b.push_raw(prefix)
    }

    pub fn push_raw(mut self, s: &str) -> Self {
        if !self.buf.is_empty() {
            // `/`
            self.buf.push(0x2f);
        }

        self.buf.extend_from_slice(s.as_bytes());
        self
    }

    pub fn push_str(self, s: &str) -> Self {
        self.push_raw(&escape(s))
    }

    pub fn push_u64(self, n: u64) -> Self {
        self.push_raw(&format!("{}", n))
    }

    pub fn done(self) -> String {
        String::from_utf8(self.buf).unwrap()
    }

    /// Re-export escape()
    pub fn escape(s: &str) -> String {
        escape(s)
    }

    /// Re-export escape_specified()
    pub fn escape_specified(s: &str, chars: &[u8]) -> String {
        escape_specified(s, chars)
    }
}

#[cfg(test)]
mod tests {
    use crate::kvapi::key_builder::KeyBuilder;

    #[test]
    fn test_key_builder() -> anyhow::Result<()> {
        let s = KeyBuilder::new_prefixed("_foo")
            .push_str("a b")
            .push_u64(5)
            .push_raw("a b")
            .done();

        assert_eq!("_foo/a%20b/5/a b", s);
        Ok(())
    }
}
