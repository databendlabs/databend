// Copyright 2021 Datafuse Labs.
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

pub trait Marshal {
    fn marshal(&self, scratch: &mut [u8]);
}

impl Marshal for u8 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self;
    }
}

impl Marshal for u16 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bytes = self.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for u32 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bytes = self.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for u64 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bytes = self.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for i8 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}

impl Marshal for i16 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bytes = self.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for i32 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bytes = self.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for i64 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bytes = self.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for f32 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = self.to_bits();
        let bytes = bits.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for f64 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = self.to_bits();
        let bytes = bits.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for bool {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}

impl Marshal for char {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = *self as u32;
        let bytes = bits.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}
