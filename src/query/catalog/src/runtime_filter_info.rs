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

use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::Receiver;
use databend_common_base::base::tokio::sync::watch::Sender;
use databend_common_expression::Expr;
use xorf::BinaryFuse16;

#[derive(Clone, Default)]
pub struct RuntimeFilterInfo {
    pub inlist: Vec<Expr<String>>,
    pub min_max: Vec<Expr<String>>,
    pub bloom: Vec<(String, BinaryFuse16)>,
}

impl Debug for RuntimeFilterInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RuntimeFilterInfo {{ inlist: {}, min_max: {}, bloom: {:?} }}",
            self.inlist
                .iter()
                .map(|e| e.sql_display())
                .collect::<Vec<String>>()
                .join(","),
            self.min_max
                .iter()
                .map(|e| e.sql_display())
                .collect::<Vec<String>>()
                .join(","),
            self.bloom
                .iter()
                .map(|(name, _)| name)
                .collect::<Vec<&String>>()
        )
    }
}

impl RuntimeFilterInfo {
    pub fn is_empty(&self) -> bool {
        self.inlist.is_empty() && self.bloom.is_empty() && self.min_max.is_empty()
    }

    pub fn is_blooms_empty(&self) -> bool {
        self.bloom.is_empty()
    }
}

pub struct RuntimeFilterReady {
    pub runtime_filter_watcher: Sender<Option<()>>,
    /// A dummy receiver to make runtime_filter_watcher channel open.
    pub _runtime_filter_dummy_receiver: Receiver<Option<()>>,
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
