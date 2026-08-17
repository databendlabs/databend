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

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::DraftVirtualColumnPathStatistics;
use jsonb::RawJsonb;
use jsonb::keypath::KeyPath;

use crate::MAX_VIRTUAL_COLUMN_PATH_STATISTICS;
use crate::io::VirtualColumnLayoutPolicy;

/// Collects block-local JSON path frequencies independently from virtual-column
/// materialization. Used by insert/update/delete writers that do not generate
/// virtual columns. Recluster/compact/refresh reuse the statistics already
/// collected by `VirtualColumnBuilder`.
#[derive(Clone)]
pub struct JsonPathStatisticsBuilder {
    variant_fields: Vec<TableField>,
    variant_offsets: Vec<usize>,
    source_paths: Vec<HashMap<String, u64>>,
    max_path_statistics: usize,
}

impl JsonPathStatisticsBuilder {
    pub fn try_create(schema: TableSchemaRef, policy: VirtualColumnLayoutPolicy) -> Result<Self> {
        let mut variant_fields = Vec::new();
        let mut variant_offsets = Vec::new();
        for (offset, field) in schema.fields.iter().enumerate() {
            if field.data_type().remove_nullable() == TableDataType::Variant {
                variant_fields.push(field.clone());
                variant_offsets.push(offset);
            }
        }
        if variant_fields.is_empty() {
            return Err(ErrorCode::VirtualColumnError(
                "JSON path statistics require at least one variant field",
            ));
        }
        let source_paths = (0..variant_fields.len()).map(|_| HashMap::new()).collect();
        Ok(Self {
            variant_fields,
            variant_offsets,
            source_paths,
            max_path_statistics: if policy.max_path_statistics == 0 {
                MAX_VIRTUAL_COLUMN_PATH_STATISTICS
            } else {
                policy
                    .max_path_statistics
                    .min(MAX_VIRTUAL_COLUMN_PATH_STATISTICS)
            },
        })
    }

    pub fn observe_path(&mut self, source_column_id: ColumnId, key_paths: &[KeyPath<'_>]) {
        let Some(source_index) = self
            .variant_fields
            .iter()
            .position(|field| field.column_id == source_column_id)
        else {
            return;
        };
        let path = jsonb::keypath::KeyPaths {
            paths: key_paths.to_vec(),
        }
        .to_owned()
        .to_canonical_path();
        *self.source_paths[source_index].entry(path).or_default() += 1;
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        for source_index in 0..self.variant_offsets.len() {
            let offset = self.variant_offsets[source_index];
            let source_column_id = self.variant_fields[source_index].column_id;
            let column = block.get_by_offset(offset);
            for row in 0..block.num_rows() {
                let ScalarRef::Variant(jsonb_bytes) = (unsafe { column.index_unchecked(row) })
                else {
                    continue;
                };
                RawJsonb::new(jsonb_bytes)
                    .visit_scalar_key_paths(true, |key_paths| {
                        self.observe_path(source_column_id, key_paths);
                        Ok(())
                    })
                    .map_err(|error| {
                        ErrorCode::VirtualColumnError(format!(
                            "failed to collect JSON path statistics: {error}"
                        ))
                    })?;
            }
        }
        Ok(())
    }

    pub fn finalize(&mut self) -> HashMap<ColumnId, DraftVirtualColumnPathStatistics> {
        let source_paths = std::mem::replace(
            &mut self.source_paths,
            (0..self.variant_fields.len())
                .map(|_| HashMap::new())
                .collect(),
        );
        let mut statistics = HashMap::new();
        for (source_field, paths) in self.variant_fields.iter().zip(source_paths) {
            if paths.is_empty() {
                continue;
            }
            let mut path_counts = paths
                .into_iter()
                .map(|(path, value_count)| (path, value_count.min(u32::MAX as u64) as u32))
                .collect::<Vec<_>>();
            let complete = path_counts.len() <= self.max_path_statistics;
            path_counts
                .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            path_counts.truncate(self.max_path_statistics);
            path_counts.sort_by(|left, right| left.0.cmp(&right.0));
            statistics.insert(source_field.column_id, DraftVirtualColumnPathStatistics {
                path_statistics_complete: complete,
                path_counts,
            });
        }
        statistics
    }
}
