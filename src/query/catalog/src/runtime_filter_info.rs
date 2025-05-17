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
use databend_common_expression::types::DataType;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Expr;
use databend_common_expression::FunctionID;
use databend_common_expression::RemoteExpr;
use xorf::BinaryFuse16;

#[derive(Clone, Debug, Default)]
pub struct RuntimeFilterInfo {
    inlist: Vec<Expr<String>>,
    min_max: Vec<Expr<String>>,
    bloom: Vec<(String, BinaryFuse16)>,
}

impl RuntimeFilterInfo {
    pub fn add_inlist(&mut self, expr: Expr<String>) {
        self.inlist.push(expr);
    }

    pub fn add_bloom(&mut self, bloom: (String, BinaryFuse16)) {
        self.bloom.push(bloom);
    }

    pub fn add_min_max(&mut self, expr: Expr<String>) {
        self.min_max.push(expr);
    }

    pub fn get_inlist(&self) -> &Vec<Expr<String>> {
        &self.inlist
    }

    pub fn get_bloom(&self) -> &Vec<(String, BinaryFuse16)> {
        &self.bloom
    }

    pub fn get_min_max(&self) -> &Vec<Expr<String>> {
        &self.min_max
    }

    pub fn blooms(self) -> Vec<(String, BinaryFuse16)> {
        self.bloom
    }

    pub fn inlists(self) -> Vec<Expr<String>> {
        self.inlist
    }

    pub fn min_maxs(self) -> Vec<Expr<String>> {
        self.min_max
    }

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

#[derive(Clone, Debug, Default)]
pub struct RuntimeFilterShard {
    pub id: usize,
    pub scan_id: usize,
    pub inlist: Option<Expr<String>>,
    pub min_max: Option<Expr<String>>,
    pub bloom: Option<(String, BinaryFuse16)>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct RemoteRuntimeFilterShard {
    pub id: usize,
    pub scan_id: usize,
    pub inlist: Option<RemoteExpr<String>>,
    pub min_max: Option<RemoteExpr<String>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct RemoteRuntimeFilterShards {
    pub shards: Option<HashMap<usize, RemoteRuntimeFilterShard>>,
}

impl RemoteRuntimeFilterShards {
    pub fn merge(
        shards: Vec<HashMap<usize, RemoteRuntimeFilterShard>>,
    ) -> HashMap<usize, RemoteRuntimeFilterShard> {
        if shards.is_empty() {
            return HashMap::new();
        }

        let mut result = HashMap::new();
        for key in shards[0].keys() {
            result.insert(*key, RemoteRuntimeFilterShard {
                id: *key,
                scan_id: shards[0][key].scan_id,
                inlist: Self::merge_inlist(&shards, *key),
                min_max: Self::merge_min_max(&shards, *key),
            });
        }

        result
    }

    fn merge_inlist(
        shards: &[HashMap<usize, RemoteRuntimeFilterShard>],
        rf_id: usize,
    ) -> Option<RemoteExpr<String>> {
        if shards
            .iter()
            .any(|shard| shard.get(&rf_id).unwrap().inlist.is_none())
        {
            return None;
        }
        let mut merged = shards[0][&rf_id].inlist.clone().unwrap();
        for shard in shards.iter().skip(1) {
            let inlist = shard[&rf_id].inlist.clone().unwrap();
            merged = Self::merge_expr(merged, inlist);
        }
        Some(merged)
    }

    fn merge_min_max(
        shards: &[HashMap<usize, RemoteRuntimeFilterShard>],
        rf_id: usize,
    ) -> Option<RemoteExpr<String>> {
        if shards
            .iter()
            .any(|shard| shard.get(&rf_id).unwrap().min_max.is_none())
        {
            return None;
        }
        let mut merged = shards[0][&rf_id].min_max.clone().unwrap();
        for shard in shards.iter().skip(1) {
            let min_max = shard[&rf_id].min_max.clone().unwrap();
            merged = Self::merge_expr(merged, min_max);
        }
        Some(merged)
    }

    fn merge_expr(l: RemoteExpr<String>, r: RemoteExpr<String>) -> RemoteExpr<String> {
        RemoteExpr::FunctionCall {
            span: None,
            id: Box::new(FunctionID::Builtin {
                name: "or".to_string(),
                id: 1,
            }),
            generics: vec![],
            args: vec![l, r],
            return_type: DataType::Nullable(Box::new(DataType::Boolean)),
        }
    }
}

#[typetag::serde(name = "remote_runtime_filter_shards")]
impl BlockMetaInfo for RemoteRuntimeFilterShards {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        RemoteRuntimeFilterShards::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl From<RuntimeFilterShard> for RemoteRuntimeFilterShard {
    fn from(shard: RuntimeFilterShard) -> Self {
        Self {
            id: shard.id,
            scan_id: shard.scan_id,
            inlist: shard.inlist.map(|expr| expr.as_remote_expr()),
            min_max: shard.min_max.map(|expr| expr.as_remote_expr()),
        }
    }
}

impl From<Option<HashMap<usize, RuntimeFilterShard>>> for RemoteRuntimeFilterShards {
    fn from(shards: Option<HashMap<usize, RuntimeFilterShard>>) -> Self {
        Self {
            shards: shards.map(|shards| {
                shards
                    .into_iter()
                    .map(|(id, shard)| (id, shard.into()))
                    .collect()
            }),
        }
    }
}
