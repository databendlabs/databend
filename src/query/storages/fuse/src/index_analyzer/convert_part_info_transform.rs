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
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_catalog::plan::PartInfo;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::BlockMetaAccumulatingTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaAccumulatingTransformer;
use common_pipeline_transforms::processors::transforms::Transform;
use serde::Deserializer;
use serde::Serializer;

use crate::index_analyzer::block_filter_meta::BlockFilterMeta;
use crate::FusePartInfo;
use crate::FuseTable;

pub struct FusePartMeta {
    pub part_info: Arc<Box<dyn PartInfo>>,
}

impl FusePartMeta {
    pub fn create(part_info: Arc<Box<dyn PartInfo>>) -> BlockMetaInfoPtr {
        Box::new(FusePartMeta { part_info })
    }
}

impl Debug for FusePartMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusePartMeta")
            // .field("location", &self.segment_location)
            .finish()
    }
}

impl serde::Serialize for FusePartMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize FusePartMeta")
    }
}

impl<'de> serde::Deserialize<'de> for FusePartMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize FusePartMeta")
    }
}

#[typetag::serde(name = "fuse_part_meta")]
impl BlockMetaInfo for FusePartMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals FusePartMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone FusePartMeta")
    }
}

pub struct PartInfoConvertTransform {
    schema: TableSchemaRef,
}

impl PartInfoConvertTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: TableSchemaRef,
    ) -> ProcessorPtr {
        ProcessorPtr::create(BlockMetaAccumulatingTransformer::create(
            input,
            output,
            PartInfoConvertTransform { schema },
        ))
    }
}

impl BlockMetaAccumulatingTransform<BlockFilterMeta> for PartInfoConvertTransform {
    const NAME: &'static str = "BlockMetaAccumulatingTransform";

    fn transform(&mut self, data: BlockFilterMeta) -> Result<Option<DataBlock>> {
        let block_meta = data.block_meta;
        let mut columns_meta = HashMap::with_capacity(block_meta.col_metas.len());

        for column_id in block_meta.col_metas.keys() {
            // ignore all deleted field
            if self.schema.is_column_deleted(*column_id) {
                continue;
            }

            // ignore column this block dose not exist
            if let Some(meta) = block_meta.col_metas.get(column_id) {
                columns_meta.insert(*column_id, meta.clone());
            }
        }

        let rows_count = block_meta.row_count;
        let location = block_meta.location.0.clone();
        let format_version = block_meta.location.1;

        Ok(Some(DataBlock::empty_with_meta(FusePartMeta::create(
            FusePartInfo::create(
                location,
                format_version,
                rows_count,
                columns_meta,
                block_meta.compression(),
                None,
                Some(data.meta_index.to_owned()),
            ),
        ))))
    }
}
