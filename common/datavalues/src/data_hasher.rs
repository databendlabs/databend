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

use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasher;
use std::hash::Hasher;

use ahash::AHasher;
use ahash::RandomState as AhashRandomState;
use seahash::SeaHasher;

/// TODO:
/// This is very slow because it involves lots of copy to keep the origin state
/// We should have our custom none-state hash functions
/// Tracked work item: https://github.com/datafuselabs/databend/issues/3897
#[derive(Clone)]
pub enum DFHasher {
    SipHasher(DefaultHasher),
    AhashHasher(AHasher),
    SeaHasher64(SeaHasher, [u64; 4]),
}

macro_rules! apply_fn {
    ($self: ident, $func: ident) => {{
        match $self {
            DFHasher::SipHasher(v) => v.$func(),
            DFHasher::AhashHasher(v) => v.$func(),
            DFHasher::SeaHasher64(v, _) => v.$func(),
        }
    }};

    ($self: ident, $func: ident, $arg: ident) => {{
        match $self {
            DFHasher::SipHasher(v) => v.$func($arg),
            DFHasher::AhashHasher(v) => v.$func($arg),
            DFHasher::SeaHasher64(v, _) => v.$func($arg),
        }
    }};
}

impl DFHasher {
    #[must_use]
    pub fn clone_initial(&self) -> Self {
        match self {
            DFHasher::SipHasher(_) => DFHasher::SipHasher(DefaultHasher::new()),
            DFHasher::AhashHasher(_) => {
                let state = AhashRandomState::new();
                DFHasher::AhashHasher(state.build_hasher())
            }
            DFHasher::SeaHasher64(_, seeds) => {
                let hasher = SeaHasher::with_seeds(seeds[0], seeds[1], seeds[2], seeds[3]);
                DFHasher::SeaHasher64(hasher, *seeds)
            }
        }
    }
}

impl Hasher for DFHasher {
    fn finish(&self) -> u64 {
        apply_fn! {self, finish}
    }

    fn write(&mut self, bytes: &[u8]) {
        apply_fn! {self, write, bytes}
    }

    fn write_u8(&mut self, i: u8) {
        apply_fn! {self, write_u8, i}
    }

    fn write_u16(&mut self, i: u16) {
        apply_fn! {self, write_u16, i}
    }

    fn write_u32(&mut self, i: u32) {
        apply_fn! {self, write_u32, i}
    }

    fn write_u64(&mut self, i: u64) {
        apply_fn! {self, write_u64, i}
    }

    fn write_u128(&mut self, i: u128) {
        apply_fn! {self, write_u128, i}
    }

    fn write_usize(&mut self, i: usize) {
        apply_fn! {self, write_usize, i}
    }

    fn write_i8(&mut self, i: i8) {
        apply_fn! {self, write_i8, i}
    }

    fn write_i16(&mut self, i: i16) {
        apply_fn! {self, write_i16, i}
    }

    fn write_i32(&mut self, i: i32) {
        apply_fn! {self, write_i32, i}
    }

    fn write_i64(&mut self, i: i64) {
        apply_fn! {self, write_i64, i}
    }

    fn write_i128(&mut self, i: i128) {
        apply_fn! {self, write_i128, i}
    }

    fn write_isize(&mut self, i: isize) {
        apply_fn! {self, write_isize, i}
    }
}
