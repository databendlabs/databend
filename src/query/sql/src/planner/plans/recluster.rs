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

use databend_common_catalog::plan::Filters;
use databend_storages_common_table_meta::table::ClusterType;
use educe::Educe;

use crate::plans::Operator;
use crate::plans::RelOp;
use crate::BindContext;
use crate::MetadataRef;

#[derive(Debug, Clone, Educe)]
#[educe(Eq, PartialEq, Hash)]
pub struct Recluster {
    pub catalog: String,
    pub database: String,
    pub table: String,

    pub limit: Option<usize>,
    #[educe(Eq(ignore), Hash(ignore))]
    pub filters: Option<Filters>,
    #[educe(Hash(ignore))]
    pub cluster_type: ClusterType,
    #[educe(Eq(ignore), PartialEq(ignore), Hash(ignore))]
    pub bind_context: Box<BindContext>,
    #[educe(Eq(ignore), PartialEq(ignore), Hash(ignore))]
    pub metadata: MetadataRef,
}

impl Operator for Recluster {
    fn rel_op(&self) -> RelOp {
        RelOp::Recluster
    }
}
