//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common_expression::TableSchema;
use common_storages_fuse::operations::common::ConflictResolveContext;
use common_storages_fuse::operations::common::DeleteConflictResolveContext;
use common_storages_fuse::operations::common::MutationGenerator;
use common_storages_fuse::operations::common::SnapshotGenerator;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;

#[test]
fn test_unresolvable_delete_conflict() {
    let mut base_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default());
    base_snapshot.segments = vec![
        ("1".to_string(), 1),
        ("2".to_string(), 1),
        ("3".to_string(), 1),
    ];

    let mut latest_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default());
    latest_snapshot.segments = vec![("1".to_string(), 1), ("4".to_string(), 1)];

    let ctx = ConflictResolveContext::Delete(Box::new(DeleteConflictResolveContext {
        removed_segments: vec![("2".to_string(), 1)],
        added_segments: vec![],
        removed_statistics: Statistics::default(),
        added_statistics: Statistics::default(),
    }));

    let mut generator = MutationGenerator::new(Arc::new(base_snapshot));
    generator.set_context(ctx);

    let result = generator.generate_new_snapshot(
        TableSchema::default(),
        None,
        Some(Arc::new(latest_snapshot)),
    );
    assert!(result.is_err());
}

#[test]
fn test_resolvable_delete_conflict() {
    let mut base_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default());
    base_snapshot.segments = vec![
        ("1".to_string(), 1),
        ("2".to_string(), 1),
        ("3".to_string(), 1),
    ];

    base_snapshot.summary = Statistics {
        row_count: 6,
        block_count: 6,
        perfect_block_count: 6,
        uncompressed_byte_size: 6,
        compressed_byte_size: 6,
        index_size: 6,
        col_stats: HashMap::new(),
    };

    let mut latest_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default());
    latest_snapshot.segments = vec![
        ("2".to_string(), 1),
        ("3".to_string(), 1),
        ("4".to_string(), 1),
    ];

    latest_snapshot.summary = Statistics {
        row_count: 9,
        block_count: 9,
        perfect_block_count: 9,
        uncompressed_byte_size: 9,
        compressed_byte_size: 9,
        index_size: 9,
        col_stats: HashMap::new(),
    };

    let removed_statistics = Statistics {
        row_count: 5,
        block_count: 5,
        perfect_block_count: 5,
        uncompressed_byte_size: 5,
        compressed_byte_size: 5,
        index_size: 5,
        col_stats: HashMap::new(),
    };

    let added_statistics = Statistics {
        row_count: 8,
        block_count: 8,
        perfect_block_count: 8,
        uncompressed_byte_size: 8,
        compressed_byte_size: 8,
        index_size: 8,
        col_stats: HashMap::new(),
    };

    let ctx = ConflictResolveContext::Delete(Box::new(DeleteConflictResolveContext {
        removed_segments: vec![("2".to_string(), 1), ("3".to_string(), 1)],
        added_segments: vec![("8".to_string(), 1)],
        removed_statistics,
        added_statistics,
    }));

    let mut generator = MutationGenerator::new(Arc::new(base_snapshot));
    generator.set_context(ctx);

    let result = generator.generate_new_snapshot(
        TableSchema::default(),
        None,
        Some(Arc::new(latest_snapshot)),
    );
    let snapshot = result.unwrap();
    let mut actual = snapshot.segments.clone();
    actual.sort();
    let expected = vec![("4".to_string(), 1), ("8".to_string(), 1)];
    assert_eq!(actual, expected);

    let actual = snapshot.summary;
    let expected = Statistics {
        row_count: 12,
        block_count: 12,
        perfect_block_count: 12,
        uncompressed_byte_size: 12,
        compressed_byte_size: 12,
        index_size: 12,
        col_stats: HashMap::new(),
    };
    assert_eq!(actual, expected);
}
