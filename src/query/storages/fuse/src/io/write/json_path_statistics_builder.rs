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
use std::hash::Hash;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_hashtable::StackHashMap;
use databend_storages_common_table_meta::meta::DraftVirtualColumnPathStatistics;
use jsonb::RawJsonb;
use jsonb::keypath::KeyPath;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use crate::MAX_VIRTUAL_COLUMN_PATH_STATISTICS;
use crate::io::VirtualColumnLayoutPolicy;

/// Collects block-local JSON path frequencies independently from virtual-column
/// materialization. Used by insert/update/delete writers that do not generate
/// virtual columns. Recluster/compact/refresh reuse the statistics already
/// collected by `VirtualColumnBuilder`.
pub struct JsonPathStatisticsBuilder {
    variant_fields: Vec<TableField>,
    variant_offsets: Vec<usize>,
    /// Canonical paths and counts, indexed by `source_path_indices`.
    source_paths: Vec<Vec<(String, u64)>>,
    /// Hash-first lookup avoids allocating an owned path for repeated observations.
    source_path_indices: Vec<StackHashMap<u128, usize, 16>>,
    /// False when a new path was discarded after reaching the per-source limit.
    source_paths_complete: Vec<bool>,
    max_path_statistics: usize,
}

impl Clone for JsonPathStatisticsBuilder {
    fn clone(&self) -> Self {
        let source_path_indices = self
            .source_path_indices
            .iter()
            .map(|indices| {
                let mut cloned = StackHashMap::with_capacity(indices.len());
                for entry in indices.iter() {
                    // SAFETY: every newly inserted entry is initialized immediately.
                    match unsafe { cloned.insert_and_entry(*entry.key()) } {
                        Ok(cloned_entry) => cloned_entry.write(*entry.get()),
                        Err(cloned_entry) => *cloned_entry.get_mut() = *entry.get(),
                    }
                }
                cloned
            })
            .collect();
        Self {
            variant_fields: self.variant_fields.clone(),
            variant_offsets: self.variant_offsets.clone(),
            source_paths: self.source_paths.clone(),
            source_path_indices,
            source_paths_complete: self.source_paths_complete.clone(),
            max_path_statistics: self.max_path_statistics,
        }
    }
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
        let source_paths = (0..variant_fields.len()).map(|_| Vec::new()).collect();
        let source_path_indices = (0..variant_fields.len())
            .map(|_| StackHashMap::with_capacity(0))
            .collect();
        let source_paths_complete = vec![true; variant_fields.len()];
        Ok(Self {
            variant_fields,
            variant_offsets,
            source_paths,
            source_path_indices,
            source_paths_complete,
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
        let mut hasher = SipHasher24::new();
        key_paths.hash(&mut hasher);
        let hash_value = hasher.finish128().into();
        if let Some(index) = self.source_path_indices[source_index].get(&hash_value) {
            self.source_paths[source_index][*index].1 += 1;
            return;
        }

        if self.source_paths[source_index].len() >= self.max_path_statistics {
            self.source_paths_complete[source_index] = false;
            return;
        }

        let path = jsonb::keypath::KeyPaths {
            paths: key_paths.to_vec(),
        }
        .to_owned()
        .to_canonical_path();
        let index = self.source_paths[source_index].len();
        self.source_paths[source_index].push((path, 1));
        unsafe {
            match self.source_path_indices[source_index].insert_and_entry(hash_value) {
                Ok(entry) | Err(entry) => *entry.get_mut() = index,
            }
        }
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
            (0..self.variant_fields.len()).map(|_| Vec::new()).collect(),
        );
        self.source_path_indices = (0..self.variant_fields.len())
            .map(|_| StackHashMap::with_capacity(0))
            .collect();
        let source_paths_complete = std::mem::replace(&mut self.source_paths_complete, vec![
                true;
                self.variant_fields
                    .len()
            ]);
        let mut statistics = HashMap::new();
        for ((source_field, paths), complete) in self
            .variant_fields
            .iter()
            .zip(source_paths)
            .zip(source_paths_complete)
        {
            if paths.is_empty() && complete {
                continue;
            }
            let mut path_counts = paths
                .into_iter()
                .map(|(path, value_count)| (path, value_count.min(u32::MAX as u64) as u32))
                .collect::<Vec<_>>();
            path_counts.sort_by(|left, right| left.0.cmp(&right.0));
            statistics.insert(source_field.column_id, DraftVirtualColumnPathStatistics {
                path_statistics_complete: complete,
                path_counts,
            });
        }
        statistics
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use databend_common_expression::TableSchema;

    use super::*;

    #[test]
    fn test_clone_rebuilds_path_indices() {
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "v",
            TableDataType::Variant,
        )]));
        let source_column_id = schema.fields[0].column_id;
        let mut builder =
            JsonPathStatisticsBuilder::try_create(schema, VirtualColumnLayoutPolicy {
                max_path_statistics: 1,
                ..Default::default()
            })
            .unwrap();
        let retained_path = [KeyPath::Name(Cow::Borrowed("a"))];
        let discarded_path = [KeyPath::Name(Cow::Borrowed("b"))];
        builder.observe_path(source_column_id, &retained_path);
        builder.observe_path(source_column_id, &discarded_path);

        let mut cloned = builder.clone();
        assert_eq!(cloned.max_path_statistics, builder.max_path_statistics);
        assert_eq!(cloned.source_paths_complete, builder.source_paths_complete);
        cloned.observe_path(source_column_id, &retained_path);

        let original_statistics = builder.finalize();
        let original = &original_statistics[&source_column_id];
        assert!(!original.path_statistics_complete);
        assert_eq!(original.path_counts, vec![("a".to_string(), 1)]);

        let cloned_statistics = cloned.finalize();
        let cloned = &cloned_statistics[&source_column_id];
        assert!(!cloned.path_statistics_complete);
        assert_eq!(cloned.path_counts, vec![("a".to_string(), 2)]);
    }
}
