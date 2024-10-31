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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_expression::Expr;
use xorf::BinaryFuse16;

#[derive(Clone, Debug, Default)]
pub struct RuntimeFilterInfo {
    min_max: Vec<(String, Expr<String>)>,
    inlist: Vec<(String, Expr<String>)>,
    bloom: Vec<(String, Arc<BinaryFuse16>)>,
}

impl RuntimeFilterInfo {
    pub fn add_min_max(&mut self, filter: (String, Expr<String>)) {
        self.min_max.push(filter);
    }

    pub fn add_inlist(&mut self, filter: (String, Expr<String>)) {
        self.inlist.push(filter);
    }

    pub fn add_bloom(&mut self, filter: (String, Arc<BinaryFuse16>)) {
        self.bloom.push(filter);
    }

    pub fn get_min_max(&self) -> &Vec<(String, Expr<String>)> {
        &self.min_max
    }

    pub fn get_inlist(&self) -> &Vec<(String, Expr<String>)> {
        &self.inlist
    }

    pub fn get_bloom(&self) -> &Vec<(String, Arc<BinaryFuse16>)> {
        &self.bloom
    }

    pub fn min_maxs(self) -> Vec<(String, Expr<String>)> {
        self.min_max
    }

    pub fn inlists(self) -> Vec<(String, Expr<String>)> {
        self.inlist
    }

    pub fn blooms(self) -> Vec<(String, Arc<BinaryFuse16>)> {
        self.bloom
    }

    pub fn is_empty(&self) -> bool {
        self.inlist.is_empty() && self.bloom.is_empty() && self.min_max.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct HashJoinProbeStatistics {
    // Statistics for runtime filter, the `num_rows` indicates the number of valid rows in probe side.
    // the `num_hash_matched_rows` indicates the number of keys which matched by hash.
    pub num_rows: AtomicU64,
    pub num_hash_matched_rows: AtomicU64,
}

impl HashJoinProbeStatistics {
    pub fn increment_num_rows(&self, num_rows: u64) {
        self.num_rows.fetch_add(num_rows, Ordering::AcqRel);
    }

    pub fn increment_num_hash_matched_rows(&self, num_hash_matched_rows: u64) {
        self.num_hash_matched_rows
            .fetch_add(num_hash_matched_rows, Ordering::AcqRel);
    }

    // Check whether to use runtime filter in table scan.
    pub fn prefer_runtime_filter(&self) -> bool {
        // If the number of valid rows in probe side is less than 1/2 of the number
        // of rows which matched by hash, we prefer to use runtime filter.
        self.num_hash_matched_rows.load(Ordering::Acquire) * 2
            < self.num_rows.load(Ordering::Acquire)
    }
}
