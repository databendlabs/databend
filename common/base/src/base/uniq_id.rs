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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub struct GlobalSequence;

impl GlobalSequence {
    pub fn next() -> usize {
        static GLOBAL_SEQ: AtomicUsize = AtomicUsize::new(0);

        GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct GlobalUniqName;

impl GlobalUniqName {
    pub fn unique() -> String {
        let mut uuid = uuid::Uuid::new_v4().as_u128();
        let mut unique_name = Vec::with_capacity(22);

        loop {
            let m = (uuid % 62) as u8;
            uuid /= 62;

            match m as u8 {
                0..=9 => unique_name.push((b'0' + m) as char),
                10..=35 => unique_name.push((b'a' + (m - 10)) as char),
                36..=61 => unique_name.push((b'A' + (m - 36)) as char),
                unreachable => unreachable!("Unreachable branch m = {}", unreachable),
            }

            if uuid == 0 {
                return unique_name.iter().collect();
            }
        }
    }
}
