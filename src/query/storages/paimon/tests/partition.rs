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

use common::TestWarehouse;
use common::scan_splits;
use common::setup_append_table;
use common::setup_pk_table;
use databend_common_catalog::plan::PartInfo;
use databend_common_storages_paimon::PaimonPartInfo;
use databend_common_storages_paimon::SerializableDataSplit;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn test_serializable_split_round_trip_from_append_scan() {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let splits = scan_splits(&wh.warehouse, &identifier).await;
    let split = splits.first().expect("split");
    assert!(!split.data_files().is_empty());

    let wire = SerializableDataSplit::from(split);
    let restored: paimon::DataSplit = wire.clone().try_into().expect("restore split");

    assert_eq!(wire.snapshot_id, split.snapshot_id());
    assert_eq!(wire.partition, *split.partition());
    assert_eq!(wire.bucket, split.bucket());
    assert_eq!(wire.bucket_path, split.bucket_path());
    assert_eq!(wire.total_buckets, split.total_buckets());
    assert_eq!(wire.data_files, split.data_files());
    assert_eq!(
        wire.deletion_files,
        split.data_deletion_files().map(|v| v.to_vec())
    );
    assert_eq!(wire.row_ranges, split.row_ranges().map(|v| v.to_vec()));
    assert_eq!(restored.data_files().len(), split.data_files().len());
}

#[tokio::test]
async fn test_pk_split_wire_round_trip_preserves_all_fields() {
    let wh = TestWarehouse::new();
    let identifier = setup_pk_table(&wh.warehouse).await;
    let splits = scan_splits(&wh.warehouse, &identifier).await;
    let split = splits.first().expect("pk split");
    assert!(!split.data_files().is_empty());

    let wire = SerializableDataSplit::from(split);
    let restored: paimon::DataSplit = wire.clone().try_into().expect("restore pk split");

    assert_eq!(wire.snapshot_id, split.snapshot_id());
    assert_eq!(wire.partition, *split.partition());
    assert_eq!(wire.bucket, split.bucket());
    assert_eq!(wire.bucket_path, split.bucket_path());
    assert_eq!(wire.total_buckets, split.total_buckets());
    assert_eq!(wire.data_files.len(), split.data_files().len());
    for (left, right) in wire.data_files.iter().zip(split.data_files()) {
        assert_eq!(left, right);
    }
    assert_eq!(
        wire.deletion_files,
        split.data_deletion_files().map(|files| files.to_vec())
    );
    assert_eq!(
        wire.row_ranges,
        split.row_ranges().map(|ranges| ranges.to_vec())
    );
    assert_eq!(restored.snapshot_id(), split.snapshot_id());
    assert_eq!(restored.bucket(), split.bucket());
    assert_eq!(restored.bucket_path(), split.bucket_path());
    assert_eq!(restored.total_buckets(), split.total_buckets());
}

#[test]
fn test_part_info_typetag_round_trip_with_optional_fields() {
    let wire = SerializableDataSplit {
        snapshot_id: 7,
        partition: paimon::spec::BinaryRow::new(0),
        bucket: 3,
        bucket_path: "db/table/bucket-3".to_string(),
        total_buckets: 8,
        data_files: vec![],
        deletion_files: Some(vec![None]),
        row_ranges: Some(vec![paimon::RowRange::new(0, 10)]),
    };
    let part = PaimonPartInfo { split: wire };
    let json = serde_json::to_string(&part).expect("serialize");
    let decoded: PaimonPartInfo = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(part, decoded);

    let dyn_part: Box<dyn PartInfo> = Box::new(part.clone());
    let other: Box<dyn PartInfo> = Box::new(decoded);
    assert!(dyn_part.equals(&other));
}
