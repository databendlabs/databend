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

use common_ast::ast::TableIndexType;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::TableInfo;
use common_meta_types::MetaId;

use crate::plans::Plan;
use crate::MetadataRef;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexPlan {
    pub if_not_exists: bool,
    pub index_type: TableIndexType,
    pub index_name: String,
    pub query: String,
    pub table_id: MetaId,
}

/// Drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropIndexPlan {
    pub if_exists: bool,
    pub index: String,
}

#[derive(Clone, Debug)]
pub struct RefreshIndexPlan {
    pub index_id: u64,
    pub index_name: String,
    pub index_meta: IndexMeta,
    pub limit: Option<u64>,
    pub table_info: TableInfo,
    pub query_plan: Box<Plan>,
    pub metadata: MetadataRef,
    pub user_defined_block_name: bool,
}
