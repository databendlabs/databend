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

/// [`KeysRef`]` is a mutable reference to a slice of bytes.
///
/// It's free to modify the fields by arbitrary data.
/// Therefore, we should be careful when using this struct.
#[derive(Debug)]
pub struct KeysRef {
    data: *mut u8,
    len: usize,
}

impl KeysRef {
    #[inline(always)]
    pub fn new(data: *mut u8) -> Self {
        Self { data, len: 0 }
    }

    #[inline(always)]
    pub fn get(&self) -> *mut u8 {
        self.data
    }

    #[inline(always)]
    pub fn set_len(&mut self, len: usize) {
        self.len = len;
    }

    /// Prepare a pointer to write data.
    ///
    /// **NOTES**: the length of the keys ref will be increased by `write_len` after calling this method.
    #[inline(always)]
    pub fn writer(&mut self, write_len: usize) -> *mut u8 {
        let pointer = unsafe { self.data.add(self.len) };
        self.len += write_len;
        pointer
    }

    #[inline(always)]
    pub fn slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }
}

pub struct KeysRefIterator<'a> {
    keys: std::slice::Iter<'a, KeysRef>,
}

impl<'a> From<std::slice::Iter<'a, KeysRef>> for KeysRefIterator<'a> {
    fn from(iter: std::slice::Iter<'a, KeysRef>) -> Self {
        Self { keys: iter }
    }
}

impl<'a> Iterator for KeysRefIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.keys.next().map(|key| key.slice())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.keys.size_hint()
    }
}

unsafe impl<'a> std::iter::TrustedLen for KeysRefIterator<'a> {}
