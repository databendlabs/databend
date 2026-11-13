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
use std::sync::Arc;

use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_fuse::FUSE_TBL_LAST_SNAPSHOT_HINT;
use databend_common_storages_fuse::FUSE_TBL_LAST_SNAPSHOT_HINT_V2;
use databend_common_storages_fuse::operations::SnapshotHint;
use databend_common_storages_fuse::operations::load_last_snapshot_hint;
use databend_storages_common_table_meta::table::OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN;
use databend_storages_common_table_meta::table::is_internal_opt_key;
use databend_storages_common_table_meta::table::is_reserved_opt_key;
use opendal::Operator;
use opendal::services::Memory;

#[tokio::test]
async fn test_snapshot_hint_truncation_settings_compatibility() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let v2_path = format!("table/{FUSE_TBL_LAST_SNAPSHOT_HINT_V2}");
    let legacy_path = format!("table/{FUSE_TBL_LAST_SNAPSHOT_HINT}");
    let old_v2 = serde_json::json!({
        "snapshot_full_path": "/table/_ss/snapshot",
        "entity_comments": {"table_comment": "", "field_comments": []}
    });
    let mut meta = TableMeta {
        // Projection/reordering must use IDs, never field positions.
        schema: Arc::new(TableSchema::new_from_column_ids(
            vec![
                TableField::new_from_column_id("b", TableDataType::String, 9),
                TableField::new_from_column_id("a", TableDataType::String, 3),
            ],
            BTreeMap::new(),
            10,
        )),
        ..Default::default()
    };
    for settings in [
        Some(BTreeMap::from([(3, 4), (7, 8), (9, 40)])),
        Some(BTreeMap::new()),
        None,
    ] {
        let mut json = old_v2.clone();
        if let Some(settings) = &settings {
            json["field_stats_truncate_len"] = serde_json::to_value(settings).unwrap();
        }
        operator
            .write(&v2_path, serde_json::to_vec(&json).unwrap())
            .await
            .unwrap();
        let hint = load_last_snapshot_hint("table", &operator)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hint.field_stats_truncate_len, settings);
        let roundtrip: SnapshotHint =
            serde_json::from_slice(&serde_json::to_vec(&hint).unwrap()).unwrap();
        assert_eq!(roundtrip.field_stats_truncate_len, settings);

        // Reuse metadata to cover removal of stale settings/known markers.
        hint.apply_stats_truncate_len(&mut meta);
        let expected = settings.as_ref().map(|settings| {
            settings
                .iter()
                .filter(|(id, _)| **id != 7)
                .map(|(&id, &len)| (id, len))
                .collect::<BTreeMap<_, _>>()
        });
        assert_eq!(meta.field_stats_truncate_len, expected.unwrap_or_default());
        assert_eq!(
            meta.options
                .contains_key(OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN),
            settings.is_some()
        );
    }

    operator.delete(&v2_path).await.unwrap();
    operator
        .write(&legacy_path, "/table/_ss/legacy_snapshot")
        .await
        .unwrap();
    let legacy = load_last_snapshot_hint("table", &operator)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(legacy.snapshot_full_path, "/table/_ss/legacy_snapshot");
    assert!(legacy.field_stats_truncate_len.is_none());
    legacy.apply_stats_truncate_len(&mut meta);
    assert!(meta.field_stats_truncate_len.is_empty());
    assert!(
        !meta
            .options
            .contains_key(OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN)
    );

    // The safety marker must neither be user-settable nor leak into SHOW CREATE.
    assert!(is_reserved_opt_key(OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN));
    assert!(is_internal_opt_key(OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN));
}
