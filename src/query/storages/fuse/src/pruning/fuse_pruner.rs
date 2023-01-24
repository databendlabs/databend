//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::RemoteExpr;
use common_expression::TableSchemaRef;
use opendal::Operator;
use storages_common_table_meta::meta::ClusterKey;
use storages_common_table_meta::meta::Location;

pub struct FusePruner {}

impl FusePruner {
    pub fn create(
        _ctx: &Arc<dyn TableContext>,
        _dal: Operator,
        _schema: TableSchemaRef,
        _push_down: &Option<PushDownInfo>,
        _cluster_key_meta: Option<ClusterKey>,
        _cluster_keys: Vec<RemoteExpr<String>>,
        _segment_locs: Vec<Location>,
    ) -> Result<FusePruner> {
        todo!()
    }
}
