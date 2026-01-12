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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VirtualColumnInfo;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use opendal::Operator;

use crate::io::read::load_virtual_column_file_meta;

pub struct VirtualColumnPruner {
    dal: Operator,
    virtual_column: VirtualColumnInfo,
}

impl VirtualColumnPruner {
    pub fn try_create(
        dal: Operator,
        push_down: &Option<PushDownInfo>,
    ) -> Result<Option<Arc<VirtualColumnPruner>>> {
        let virtual_column = push_down.as_ref().and_then(|p| p.virtual_column.as_ref());
        if let Some(virtual_column) = virtual_column {
            return Ok(Some(Arc::new(VirtualColumnPruner {
                dal,
                virtual_column: virtual_column.clone(),
            })));
        }
        Ok(None)
    }

    #[async_backtrace::framed]
    pub async fn prune_virtual_columns(
        &self,
        _row_count: u64,
        virtual_block_meta: &Option<VirtualBlockMeta>,
    ) -> Result<Option<VirtualBlockMetaIndex>> {
        let Some(virtual_block_meta) = virtual_block_meta else {
            return Ok(None);
        };
        if virtual_block_meta.virtual_column_size == 0 {
            return Ok(None);
        }

        let virtual_meta =
            load_virtual_column_file_meta(self.dal.clone(), &virtual_block_meta.virtual_location.0)
                .await?;

        let mut virtual_column_metas = BTreeMap::new();
        let mut shared_virtual_column_metas = BTreeMap::new();
        let mut need_source_column_ids = HashSet::new();

        for virtual_column_field in &self.virtual_column.virtual_column_fields {
            if let Some(virtual_column_meta) = virtual_meta.columns.get(&virtual_column_field.name)
            {
                let data_type = virtual_meta
                    .data_types
                    .get(&virtual_column_field.name)
                    .unwrap();
                let data_type_code = match data_type.remove_nullable() {
                    DataType::Variant => 0,
                    DataType::Boolean => 1,
                    DataType::Number(NumberDataType::UInt64) => 2,
                    DataType::Number(NumberDataType::Int64) => 3,
                    DataType::Number(NumberDataType::Float64) => 4,
                    DataType::String => 5,
                    _ => unreachable!(),
                };

                let meta = VirtualColumnMeta {
                    offset: virtual_column_meta.offset,
                    len: virtual_column_meta.len,
                    num_values: virtual_column_meta.num_values,
                    data_type: data_type_code,
                    column_stat: None,
                };

                virtual_column_metas.insert(virtual_column_field.column_id, meta);
                continue;
            }
            // read shared virtual column meta
            if let Some(shared_names) = virtual_meta
                .shared_names
                .get(&virtual_column_field.source_column_id)
            {
                if shared_names.contains(&virtual_column_field.name) {
                    if shared_virtual_column_metas
                        .contains_key(&virtual_column_field.source_column_id)
                    {
                        continue;
                    }
                    let shared_key = format!(
                        "{}.__shared_virtual_column_data__.entries.key",
                        virtual_column_field.source_column_id
                    );
                    let shared_value = format!(
                        "{}.__shared_virtual_column_data__.entries.value",
                        virtual_column_field.source_column_id
                    );

                    if let (Some(shared_key_meta), Some(shared_value_meta)) = (
                        virtual_meta.columns.get(&shared_key),
                        virtual_meta.columns.get(&shared_value),
                    ) {
                        let key_meta = VirtualColumnMeta {
                            offset: shared_key_meta.offset,
                            len: shared_key_meta.len,
                            num_values: shared_key_meta.num_values,
                            data_type: 0,
                            column_stat: None,
                        };
                        let value_meta = VirtualColumnMeta {
                            offset: shared_value_meta.offset,
                            len: shared_value_meta.len,
                            num_values: shared_value_meta.num_values,
                            data_type: 0,
                            column_stat: None,
                        };

                        shared_virtual_column_metas.insert(
                            virtual_column_field.source_column_id,
                            (key_meta, value_meta),
                        );
                        continue;
                    }
                }
            }

            // The virtual column does not exist and must be generated from the source column.
            need_source_column_ids.insert(virtual_column_field.source_column_id);
        }

        // The remaining source column can be ignored.
        let mut ignored_source_column_ids = HashSet::new();
        for column_id in self
            .virtual_column
            .source_column_ids
            .difference(&need_source_column_ids)
        {
            ignored_source_column_ids.insert(*column_id);
        }

        if !virtual_column_metas.is_empty() {
            let virtual_block_meta = VirtualBlockMetaIndex {
                virtual_block_location: virtual_block_meta.virtual_location.0.clone(),
                virtual_column_metas,
                shared_virtual_column_names: virtual_meta.shared_names.clone(),
                shared_virtual_column_metas,
                ignored_source_column_ids,
            };
            return Ok(Some(virtual_block_meta));
        }
        Ok(None)
    }
}
