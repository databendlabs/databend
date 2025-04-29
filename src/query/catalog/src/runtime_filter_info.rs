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

use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::Receiver;
use databend_common_base::base::tokio::sync::watch::Sender;
use databend_common_expression::Expr;
use xorf::BinaryFuse16;

#[derive(Clone, Debug, Default)]
pub struct RuntimeFiltersForScan {
    pub inlist: HashMap<usize, Expr<String>>,
    pub min_max: HashMap<usize, Expr<String>>,
    pub bloom: HashMap<usize, (String, BinaryFuse16)>,
}

impl RuntimeFiltersForScan {
    pub fn add_inlist(&mut self, rf_id: usize, expr: Expr<String>) {
        self.inlist.insert(rf_id, expr);
    }

    pub fn add_bloom(&mut self, rf_id: usize, bloom: (String, BinaryFuse16)) {
        self.bloom.insert(rf_id, bloom);
    }

    pub fn add_min_max(&mut self, rf_id: usize, expr: Expr<String>) {
        self.min_max.insert(rf_id, expr);
    }

    pub fn is_empty(&self) -> bool {
        self.inlist.is_empty() && self.bloom.is_empty() && self.min_max.is_empty()
    }

    pub fn is_blooms_empty(&self) -> bool {
        self.bloom.is_empty()
    }
}

pub struct RuntimeFilterReady {
    pub runtime_filter_watcher: Sender<Option<bool>>,
    /// A dummy receiver to make runtime_filter_watcher channel open.
    pub _runtime_filter_dummy_receiver: Receiver<Option<bool>>,
}

impl Default for RuntimeFilterReady {
    fn default() -> Self {
        let (watcher, dummy_receiver) = watch::channel(None);
        Self {
            runtime_filter_watcher: watcher,
            _runtime_filter_dummy_receiver: dummy_receiver,
        }
    }
}
