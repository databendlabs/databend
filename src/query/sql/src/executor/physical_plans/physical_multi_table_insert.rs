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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::executor::physical_plan::PhysicalPlanDeriveHandle;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanMeta;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Duplicate {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub n: usize,
}

#[typetag::serde]
impl IPhysicalPlan for Duplicate {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_duplicate = self.clone();
                assert_eq!(children.len(), 1);
                new_duplicate.input = children[0];
                Box::new(new_duplicate)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Shuffle {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub strategy: ShuffleStrategy,
}

#[typetag::serde]
impl IPhysicalPlan for Shuffle {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_shuffle = self.clone();
                assert_eq!(children.len(), 1);
                new_shuffle.input = children[0];
                Box::new(new_shuffle)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ShuffleStrategy {
    Transpose(usize),
}

impl ShuffleStrategy {
    pub fn shuffle(&self, total: usize) -> Result<Vec<usize>> {
        match self {
            ShuffleStrategy::Transpose(n) => {
                if total % n != 0 {
                    return Err(ErrorCode::Internal(format!(
                        "total rows {} is not divisible by n {}",
                        total, n
                    )));
                }
                let mut result = vec![0; total];
                for i in 0..*n {
                    for j in 0..total / n {
                        result[i + j * n] = i * (total / n) + j;
                    }
                }
                Ok(result)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkFilter {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub predicates: Vec<Option<RemoteExpr>>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkFilter {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_chunk_filter = self.clone();
                assert_eq!(children.len(), 1);
                new_chunk_filter.input = children[0];
                Box::new(new_chunk_filter)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkEvalScalar {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub eval_scalars: Vec<Option<MultiInsertEvalScalar>>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkEvalScalar {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_chunk_eval_scalar = self.clone();
                assert_eq!(children.len(), 1);
                new_chunk_eval_scalar.input = children[0];
                Box::new(new_chunk_eval_scalar)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MultiInsertEvalScalar {
    pub remote_exprs: Vec<RemoteExpr>,
    pub projection: ColumnSet,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkCastSchema {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub cast_schemas: Vec<Option<CastSchema>>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkCastSchema {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_chunk_cast_schema = self.clone();
                assert_eq!(children.len(), 1);
                new_chunk_cast_schema.input = children[0];
                Box::new(new_chunk_cast_schema)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CastSchema {
    pub source_schema: DataSchemaRef,
    pub target_schema: DataSchemaRef,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkFillAndReorder {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub fill_and_reorders: Vec<Option<FillAndReorder>>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkFillAndReorder {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_chunk_fill_and_reorder = self.clone();
                assert_eq!(children.len(), 1);
                new_chunk_fill_and_reorder.input = children[0];
                Box::new(new_chunk_fill_and_reorder)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FillAndReorder {
    pub source_schema: DataSchemaRef,
    pub target_table_info: TableInfo,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkAppendData {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub target_tables: Vec<SerializableTable>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkAppendData {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_chunk_append_data = self.clone();
                assert_eq!(children.len(), 1);
                new_chunk_append_data.input = children[0];
                Box::new(new_chunk_append_data)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SerializableTable {
    pub target_catalog_info: Arc<CatalogInfo>,
    pub target_table_info: TableInfo,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkMerge {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub group_ids: Vec<u64>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkMerge {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_chunk_merge = self.clone();
                assert_eq!(children.len(), 1);
                new_chunk_merge.input = children[0];
                Box::new(new_chunk_merge)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkCommitInsert {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub update_stream_meta: Vec<UpdateStreamMetaReq>,
    pub overwrite: bool,
    pub deduplicated_label: Option<String>,
    pub targets: Vec<SerializableTable>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkCommitInsert {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_chunk_commit_insert = self.clone();
                assert_eq!(children.len(), 1);
                new_chunk_commit_insert.input = children[0];
                Box::new(new_chunk_commit_insert)
            }
        }
    }
}
