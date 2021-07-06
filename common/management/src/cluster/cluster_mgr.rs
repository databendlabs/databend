// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::Result;
use common_store_api::KVApi;

use crate::cluster::ClusterMeta;

pub struct ClusterMgr<KV> {
    kv_api: KV,
}

impl<T> ClusterMgr<T>
where T: KVApi
{
    #[allow(dead_code)]
    pub fn new(kv_api: T) -> Self {
        ClusterMgr { kv_api }
    }

    pub async fn upsert_meta(&mut self, _namespace: &str, _meta: ClusterMeta) -> Result<()> {
        todo!()
    }

    pub async fn get_metas(&mut self, _namespace: &str) -> Result<Vec<ClusterMeta>> {
        todo!()
    }
}
