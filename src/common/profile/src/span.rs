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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

pub type ProfSpanSetRef<K = u32> = Arc<Mutex<ProfSpanSet<K>>>;

#[derive(Default)]
pub struct ProfSpan {
    /// The time spent to process in nanoseconds
    pub process_time: u64,
}

impl ProfSpan {
    pub fn add(&mut self, other: &Self) {
        self.process_time += other.process_time;
    }
}

#[derive(Default)]
pub struct ProfSpanSet<K = u32> {
    spans: HashMap<K, ProfSpan>,
}

impl<K> ProfSpanSet<K>
where K: std::hash::Hash + Eq
{
    pub fn update(&mut self, key: K, span: ProfSpan) {
        let entry = self.spans.entry(key).or_insert_with(ProfSpan::default);
        entry.add(&span);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &ProfSpan)> {
        self.spans.iter()
    }

    pub fn get(&self, k: &K) -> Option<&ProfSpan> {
        self.spans.get(k)
    }
}

#[derive(Clone, Default)]
pub struct ProfSpanBuilder {
    process_time: u64,
}

impl ProfSpanBuilder {
    pub fn accumulate_process_time(&mut self, nanos: u64) {
        self.process_time += nanos;
    }

    pub fn finish(self) -> ProfSpan {
        ProfSpan {
            process_time: self.process_time,
        }
    }
}
