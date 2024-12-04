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

use databend_common_catalog::table::CompactionLimits;
use databend_common_catalog::table::NavigationPoint;

use crate::plans::Operator;
use crate::plans::RelOp;
use crate::BindContext;
use crate::MetadataRef;

#[derive(Clone, Debug)]
pub struct OptimizePurgePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub instant: Option<NavigationPoint>,
    pub num_snapshot_limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct OptimizeCompactSegmentPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub num_segment_limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OptimizeCompactBlock {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub limit: CompactionLimits,
}

impl Operator for OptimizeCompactBlock {
    fn rel_op(&self) -> RelOp {
        RelOp::CompactBlock
    }
}

#[derive(Clone)]
pub struct OptimizeClusterBy {
    pub catalog_name: String,
    pub database_name: String,
    pub table_name: String,
    pub metadata: MetadataRef,
    pub bind_context: Box<BindContext>,
}

impl std::fmt::Debug for OptimizeClusterBy {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("OptimizeClusterBy")
            .field("catalog", &self.catalog_name)
            .field("database", &self.database_name)
            .field("table", &self.table_name)
            .finish()
    }
}

impl Eq for OptimizeClusterBy {}

impl PartialEq for OptimizeClusterBy {
    fn eq(&self, other: &Self) -> bool {
        self.catalog_name == other.catalog_name
            && self.database_name == other.database_name
            && self.table_name == other.table_name
    }
}

impl std::hash::Hash for OptimizeClusterBy {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.database_name.hash(state);
        self.table_name.hash(state);
    }
}

impl Operator for OptimizeClusterBy {
    fn rel_op(&self) -> RelOp {
        RelOp::OptimizeClusterBy
    }
}
