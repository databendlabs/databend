// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common_expression::RemoteExpr;
use common_sql::plans::RuntimeFilterId;
use storages_common_index::filters::Xor8Filter;

use crate::pipelines::processors::transforms::runtime_filter::RuntimeFilterConnector;

pub struct RuntimeFilterState {
    pub(crate) channel_filters: Arc<Mutex<HashMap<RuntimeFilterId, Xor8Filter>>>,
    pub(crate) left_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    pub(crate) right_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
}

impl RuntimeFilterState {
    pub fn new(
        left_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
        right_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    ) -> Self {
        RuntimeFilterState {
            channel_filters: Arc::new(Mutex::new(Default::default())),
            left_runtime_filters,
            right_runtime_filters,
        }
    }
}

impl RuntimeFilterConnector for RuntimeFilterState {}
