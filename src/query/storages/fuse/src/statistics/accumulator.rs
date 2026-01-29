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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VirtualDataField;
use databend_common_expression::VirtualDataSchema;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::column_oriented_segment::*;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;

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
        additional_stats_meta: Option<AdditionalStatsMeta>,
    ) -> Result<Self::Segment> {
        let builder = std::mem::take(self);
        let mut stat =
            super::reduce_block_metas(&builder.blocks_metas, thresholds, default_cluster_key_id);
        stat.additional_stats_meta = additional_stats_meta;
        Ok(SegmentInfo::new(builder.blocks_metas, stat))
    }

    fn new(_table_schema: TableSchemaRef, _block_per_segment: usize) -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct VirtualColumnAccumulator {
    virtual_fields: BTreeMap<(ColumnId, String), usize>,
    virtual_schema: VirtualDataSchema,
    number_of_blocks: u64,
}

impl VirtualColumnAccumulator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        schema: &Arc<TableSchema>,
        virtual_schema: &Option<VirtualDataSchema>,
    ) -> Option<VirtualColumnAccumulator> {
        if !ctx
            .get_settings()
            .get_enable_experimental_virtual_column()
            .unwrap_or_default()
            || LicenseManagerSwitch::instance()
                .check_enterprise_enabled(ctx.get_license_key(), Feature::VirtualColumn)
                .is_err()
        {
            return None;
        }

        let has_variant = schema
            .fields
            .iter()
            .any(|f| matches!(f.data_type.remove_nullable(), TableDataType::Variant));
        if !has_variant {
            return None;
        }

        let mut virtual_fields = BTreeMap::new();
        let virtual_schema = if let Some(virtual_schema) = virtual_schema {
            for (i, virtual_field) in virtual_schema.fields.iter().enumerate() {
                let key = (virtual_field.source_column_id, virtual_field.name.clone());
                virtual_fields.insert(key, i);
            }
            virtual_schema.clone()
        } else {
            VirtualDataSchema::empty()
        };

        Some(VirtualColumnAccumulator {
            virtual_fields,
            virtual_schema,
            number_of_blocks: 0,
        })
    }

    pub fn add_virtual_column_metas(
        &mut self,
        draft_virtual_column_metas: &Vec<DraftVirtualColumnMeta>,
    ) -> HashMap<ColumnId, VirtualColumnMeta> {
        let mut virtual_column_metas = HashMap::new();

        for draft_virtual_column_meta in draft_virtual_column_metas {
            let key = (
                draft_virtual_column_meta.source_column_id,
                draft_virtual_column_meta.name.clone(),
            );

            let column_id = if let Some(field_idx) = self.virtual_fields.get(&key) {
                let virtual_field =
                    unsafe { self.virtual_schema.fields.get_unchecked_mut(*field_idx) };
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
                if self.virtual_schema.is_full() {
                    continue;
                }
                self.virtual_fields
                    .insert(key, self.virtual_schema.num_fields());

                let new_virtual_field = VirtualDataField {
                    name: draft_virtual_column_meta.name.clone(),
                    data_types: vec![draft_virtual_column_meta.data_type.clone()],
                    source_column_id: draft_virtual_column_meta.source_column_id,
                    column_id: 0,
                };
                self.virtual_schema.add_field(new_virtual_field).unwrap()
            };
            virtual_column_metas.insert(column_id, draft_virtual_column_meta.column_meta.clone());
        }
        self.number_of_blocks += 1;

        virtual_column_metas
    }

    pub fn build_virtual_schema(self) -> Option<VirtualDataSchema> {
        if self.virtual_schema.num_fields() > 0 {
            Some(self.virtual_schema)
        } else {
            None
        }
    }

    pub fn build_virtual_schema_with_block_number(mut self) -> Option<VirtualDataSchema> {
        if self.virtual_schema.num_fields() > 0 {
            self.virtual_schema.number_of_blocks += self.number_of_blocks;
            Some(self.virtual_schema)
        } else {
            None
        }
    }
}

#[derive(Default)]
pub struct ColumnHLLAccumulator {
    pub hlls: Vec<RawBlockHLL>,
    pub summary: BlockHLL,
}

impl ColumnHLLAccumulator {
    pub fn add_hll(&mut self, hll: BlockHLLState) -> Result<()> {
        match hll {
            BlockHLLState::Deserialized(v) => {
                let data = encode_column_hll(&v)?;
                self.hlls.push(data);
                merge_column_hll_mut(&mut self.summary, &v);
            }
            BlockHLLState::Serialized(v) => self.hlls.push(v),
        }
        Ok(())
    }

    pub fn build(&mut self) -> SegmentStatistics {
        SegmentStatistics::new(std::mem::take(&mut self.hlls))
    }

    pub fn is_empty(&self) -> bool {
        self.hlls.is_empty()
    }

    pub fn take_summary(&mut self) -> BlockHLL {
        std::mem::take(&mut self.summary)
    }
}
