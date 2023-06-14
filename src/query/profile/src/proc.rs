// Copyright 2023 Datafuse Labs
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
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

pub type SharedProcessorProfiles = Arc<Mutex<ProcessorProfiles<u32>>>;

/// Execution profile information of a `Processor`.
/// Can be merged with other `ProcessorProfile` using
/// `add` or `+` operator.
///
/// # Example
/// ```
/// let profile1 = ProcessorProfile::default();
/// let profile2 = ProcessorProfile::default();
/// let profile = profile1 + profile2;
/// ```
#[derive(Default, Clone, Copy, Debug)]
pub struct ProcessorProfile {
    /// The time spent to process in nanoseconds
    pub cpu_time: Duration,
}

impl std::ops::Add for ProcessorProfile {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            cpu_time: self.cpu_time + rhs.cpu_time,
        }
    }
}

#[derive(Default)]
pub struct ProcessorProfiles<K = u32> {
    spans: HashMap<K, ProcessorProfile>,
}

impl<K> ProcessorProfiles<K>
where K: std::hash::Hash + Eq + PartialEq + Clone + Debug
{
    pub fn update(&mut self, key: K, span: ProcessorProfile) {
        let entry = self
            .spans
            .entry(key)
            .or_insert_with(ProcessorProfile::default);
        *entry = *entry + span;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &ProcessorProfile)> {
        self.spans.iter()
    }

    pub fn get(&self, k: &K) -> Option<&ProcessorProfile> {
        self.spans.get(k)
    }
}
