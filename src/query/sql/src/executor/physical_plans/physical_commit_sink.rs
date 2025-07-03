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

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::executor::physical_plan::DeriveHandle;
use crate::executor::physical_plans::common::MutationKind;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanMeta;
use crate::plans::TruncateMode;

// serde is required by `PhysicalPlan`
/// The commit sink is used to commit the data to the table.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CommitSink {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub snapshot: Option<Arc<TableSnapshot>>,
    pub table_info: TableInfo,
    pub commit_type: CommitType,
    pub update_stream_meta: Vec<UpdateStreamMetaReq>,
    pub deduplicated_label: Option<String>,
    pub table_meta_timestamps: TableMetaTimestamps,

    // Used for recluster.
    pub recluster_info: Option<ReclusterInfoSideCar>,
}

#[typetag::serde]
impl IPhysicalPlan for CommitSink {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum CommitType {
    Truncate {
        mode: TruncateMode,
    },
    Mutation {
        kind: MutationKind,
        merge_meta: bool,
    },
}
