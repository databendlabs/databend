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

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::TestWarehouse;
use common::filesystem_catalog;
use common::setup_metadata_table;
use common::setup_multi_split_append_table;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberScalar;
use databend_common_storages_paimon::PaimonSystemTableKind;
use databend_common_storages_paimon::read_system_table;
use paimon::Catalog;

/// Collects a string/nullable-string column as `Vec<Option<String>>`.
fn string_column(block: &DataBlock, offset: usize) -> Vec<Option<String>> {
    let entry = block.get_by_offset(offset);
    (0..block.num_rows())
        .map(|row| match entry.index(row).expect("row in range") {
            ScalarRef::String(value) => Some(value.to_string()),
            ScalarRef::Null => None,
            other => panic!("expected string scalar, got {other:?}"),
        })
        .collect()
}

/// Reads a non-null `Int64` cell.
fn int64_at(block: &DataBlock, offset: usize, row: usize) -> i64 {
    match block
        .get_by_offset(offset)
        .index(row)
        .expect("row in range")
    {
        ScalarRef::Number(NumberScalar::Int64(value)) => value,
        other => panic!("expected int64 scalar, got {other:?}"),
    }
}

#[tokio::test]
async fn simple_metadata() {
    let warehouse = TestWarehouse::new();
    let identifier = setup_metadata_table(&warehouse.warehouse).await;
    let catalog = Arc::new(filesystem_catalog(&warehouse.warehouse));
    let table = catalog.get_table(&identifier).await.expect("get table");

    for (kind, expected_rows) in [
        (PaimonSystemTableKind::Options, 1),
        (PaimonSystemTableKind::Snapshots, 2),
        (PaimonSystemTableKind::Schemas, 1),
        (PaimonSystemTableKind::Branches, 1),
        (PaimonSystemTableKind::Tags, 1),
    ] {
        let block = read_system_table(kind, catalog.clone(), identifier.clone(), table.clone())
            .await
            .unwrap_or_else(|err| panic!("read {kind:?}: {err}"));
        assert_eq!(block.num_columns(), kind.schema().num_fields(), "{kind:?}");
        assert_eq!(block.num_rows(), expected_rows, "{kind:?}");
    }
}

#[tokio::test]
async fn files_manifests_partitions() {
    let warehouse = TestWarehouse::new();
    let identifier = setup_multi_split_append_table(&warehouse.warehouse).await;
    let catalog = Arc::new(filesystem_catalog(&warehouse.warehouse));
    let table = catalog.get_table(&identifier).await.expect("get table");

    let files = read_system_table(
        PaimonSystemTableKind::Files,
        catalog.clone(),
        identifier.clone(),
        table.clone(),
    )
    .await
    .expect("read files");
    assert_eq!(
        files.num_columns(),
        PaimonSystemTableKind::Files.schema().num_fields()
    );
    let record_count: i64 = match files.get_by_offset(6).value() {
        Value::Column(Column::Number(NumberColumn::Int64(values))) => values.iter().sum(),
        value => panic!("unexpected record_count column: {value:?}"),
    };
    assert_eq!(record_count, 4);

    // One data file per partition; the partition column decodes the partition BinaryRow.
    let partitions: BTreeSet<Option<String>> = string_column(&files, 0).into_iter().collect();
    assert_eq!(
        partitions,
        BTreeSet::from([
            Some("[0]".to_string()),
            Some("[1]".to_string()),
            Some("[2]".to_string()),
            Some("[3]".to_string()),
        ])
    );
    // Append-only table carries no trimmed primary key, so min/max_key stay null.
    assert!(string_column(&files, 8).iter().all(Option::is_none));
    assert!(string_column(&files, 9).iter().all(Option::is_none));
    // The Rust append writer emits empty value stats, which decode to `{}`.
    assert!(
        string_column(&files, 11)
            .iter()
            .all(|value| value.as_deref() == Some("{}"))
    );

    let manifests = read_system_table(
        PaimonSystemTableKind::Manifests,
        catalog.clone(),
        identifier.clone(),
        table.clone(),
    )
    .await
    .expect("read manifests");
    assert_eq!(
        manifests.num_columns(),
        PaimonSystemTableKind::Manifests.schema().num_fields()
    );
    assert!(manifests.num_rows() > 0);
    // Manifest partition stats exercise the real BinaryTableStats decoder: each
    // per-commit manifest covers a single partition, so min == max == {"part":k}.
    let min_partition_stats: BTreeSet<Option<String>> =
        string_column(&manifests, 5).into_iter().collect();
    assert_eq!(
        min_partition_stats,
        BTreeSet::from([
            Some("{\"part\":0}".to_string()),
            Some("{\"part\":1}".to_string()),
            Some("{\"part\":2}".to_string()),
            Some("{\"part\":3}".to_string()),
        ])
    );
    assert_eq!(
        string_column(&manifests, 6)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        min_partition_stats
    );

    let expected_partitions = catalog
        .list_partitions(&identifier)
        .await
        .expect("list partitions");
    let partitions = read_system_table(
        PaimonSystemTableKind::Partitions,
        catalog,
        identifier,
        table,
    )
    .await
    .expect("read partitions");
    assert_eq!(
        partitions.num_columns(),
        PaimonSystemTableKind::Partitions.schema().num_fields()
    );
    assert_eq!(partitions.num_rows(), expected_partitions.len());
    assert_eq!(partitions.num_rows(), 4);
    assert_eq!(
        string_column(&partitions, 0)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            Some("[0]".to_string()),
            Some("[1]".to_string()),
            Some("[2]".to_string()),
            Some("[3]".to_string()),
        ])
    );
}

#[tokio::test]
async fn all_kinds_match_registered_schema() {
    let warehouse = TestWarehouse::new();
    let identifier = setup_multi_split_append_table(&warehouse.warehouse).await;
    let catalog = Arc::new(filesystem_catalog(&warehouse.warehouse));
    let table = catalog.get_table(&identifier).await.expect("get table");

    // Every registered kind must read without error and emit exactly the columns
    // declared by its schema, guarding against collector/schema drift.
    for kind in PaimonSystemTableKind::ALL {
        let block = read_system_table(kind, catalog.clone(), identifier.clone(), table.clone())
            .await
            .unwrap_or_else(|err| panic!("read {kind:?}: {err}"));
        assert_eq!(
            block.num_columns(),
            kind.schema().num_fields(),
            "{kind:?} column count"
        );
    }
}

#[tokio::test]
async fn size_and_indexes() {
    let warehouse = TestWarehouse::new();
    let identifier = setup_multi_split_append_table(&warehouse.warehouse).await;
    let catalog = Arc::new(filesystem_catalog(&warehouse.warehouse));
    let table = catalog.get_table(&identifier).await.expect("get table");

    let physical = read_system_table(
        PaimonSystemTableKind::PhysicalFilesSize,
        catalog.clone(),
        identifier.clone(),
        table.clone(),
    )
    .await
    .expect("read physical_files_size");
    assert_eq!(
        physical.num_columns(),
        PaimonSystemTableKind::PhysicalFilesSize
            .schema()
            .num_fields()
    );
    assert_eq!(physical.num_rows(), 1);
    // Four commits produce manifest and data files on disk.
    assert!(int64_at(&physical, 0, 0) > 0, "manifest_file_count");
    assert!(int64_at(&physical, 2, 0) > 0, "data_file_count");

    let referenced = read_system_table(
        PaimonSystemTableKind::ReferencedFilesSize,
        catalog.clone(),
        identifier.clone(),
        table.clone(),
    )
    .await
    .expect("read referenced_files_size");
    assert_eq!(
        referenced.num_columns(),
        PaimonSystemTableKind::ReferencedFilesSize
            .schema()
            .num_fields()
    );
    assert!(referenced.num_rows() > 0);
    // Rows are ordered by source, and at least one source references data files.
    let sources: Vec<Option<String>> = string_column(&referenced, 0);
    let mut sorted = sources.clone();
    sorted.sort();
    assert_eq!(sources, sorted, "referenced rows sorted by source");
    assert!(
        (0..referenced.num_rows()).any(|row| int64_at(&referenced, 3, row) > 0),
        "some source references data files"
    );

    let indexes = read_system_table(
        PaimonSystemTableKind::TableIndexes,
        catalog,
        identifier,
        table,
    )
    .await
    .expect("read table_indexes");
    assert_eq!(
        indexes.num_columns(),
        PaimonSystemTableKind::TableIndexes.schema().num_fields()
    );
    // The Rust append writer produces no deletion-vector/index files.
    assert_eq!(indexes.num_rows(), 0);
}
