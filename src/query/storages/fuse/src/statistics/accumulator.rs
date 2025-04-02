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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
<<<<<<< HEAD
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::column_oriented_segment::*;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
=======
use databend_common_expression::ColumnId;
use databend_common_expression::VirtualDataField;
use databend_common_expression::VirtualDataSchema;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
>>>>>>> 31bba40384 (refactor(query): auto generate virtual columns for variant column)

#[derive(Default)]
pub struct RowOrientedSegmentBuilder {
    pub blocks_metas: Vec<Arc<BlockMeta>>,
}

impl SegmentBuilder for RowOrientedSegmentBuilder {
    type Segment = SegmentInfo;
    fn block_count(&self) -> usize {
        self.blocks_metas.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()> {
        self.blocks_metas.push(Arc::new(block_meta));
        Ok(())
    }

    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Self::Segment> {
        let builder = std::mem::take(self);
        let stat =
            super::reduce_block_metas(&builder.blocks_metas, thresholds, default_cluster_key_id);
        Ok(SegmentInfo::new(builder.blocks_metas, stat))
    }

    fn new(_table_schema: TableSchemaRef, _block_per_segment: usize) -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct VirtualColumnAccumulator {
    virtual_fields: BTreeMap<(ColumnId, String), usize>,
    pub virtual_schema: VirtualDataSchema,
}

impl VirtualColumnAccumulator {
    pub fn new(virtual_schema: &Option<VirtualDataSchema>) -> VirtualColumnAccumulator {
        let mut virtual_fields = BTreeMap::new();
        let virtual_schema = if let Some(virtual_schema) = virtual_schema {
            for (i, virtual_field) in virtual_schema.fields.iter().enumerate() {
                let key = (virtual_field.source_column_id, virtual_field.name.clone());
                virtual_fields.insert(key, i);
            }
            virtual_schema.clone()
        } else {
            VirtualDataSchema {
                fields: vec![],
                metadata: Default::default(),
                next_column_id: 3000000001,
                number_of_blocks: 0,
            }
        };

        VirtualColumnAccumulator {
            virtual_fields,
            virtual_schema,
        }
    }

    pub fn add_virtual_column_meta(
        &mut self,
        draft_virtual_column_meta: &DraftVirtualColumnMeta,
        virtual_col_metas: &mut HashMap<ColumnId, VirtualColumnMeta>,
    ) {
        let key = (
            draft_virtual_column_meta.source_column_id,
            draft_virtual_column_meta.name.clone(),
        );

        let column_id = if let Some(field_idx) = self.virtual_fields.get(&key) {
            let virtual_field = unsafe { self.virtual_schema.fields.get_unchecked_mut(*field_idx) };
            if !virtual_field
                .data_types
                .contains(&draft_virtual_column_meta.data_type)
            {
                virtual_field
                    .data_types
                    .push(draft_virtual_column_meta.data_type.clone());
            }
            virtual_field.column_id
        } else {
            let new_virtual_field = VirtualDataField {
                name: draft_virtual_column_meta.name.clone(),
                data_types: vec![draft_virtual_column_meta.data_type.clone()],
                source_column_id: draft_virtual_column_meta.source_column_id,
                column_id: self.virtual_schema.next_column_id,
            };
            let new_column_id = new_virtual_field.column_id;
            self.virtual_fields
                .insert(key, self.virtual_schema.fields.len());
            self.virtual_schema.next_column_id += 1;
            self.virtual_schema.fields.push(new_virtual_field);
            new_column_id
        };

        virtual_col_metas.insert(column_id, draft_virtual_column_meta.column_meta.clone());
    }
}
