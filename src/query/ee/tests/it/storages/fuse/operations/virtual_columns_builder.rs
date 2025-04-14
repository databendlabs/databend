// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::VariantType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Value;
use databend_common_expression::VariantDataType;
use databend_common_storages_fuse::io::VirtualColumnBuilder;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use jsonb::OwnedJsonb;

#[tokio::test(flavor = "multi_thread")]
async fn test_virtual_column_builder() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;

    let ctx = fixture.new_query_ctx().await?;

    let table = fixture.latest_default_table().await?;
    let table_info = table.get_table_info();
    let table_meta = &table_info.meta;

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let write_settings = fuse_table.get_write_settings();
    let location = (
        "_b/h0196236b460676369cfcf6fec0dedefa_v2.parquet".to_string(),
        0,
    ); // Dummy location

    let builder = VirtualColumnBuilder::try_create(ctx, table_meta).unwrap();

    let block = DataBlock::new(
        vec![
            BlockEntry::new(
                DataType::Number(NumberDataType::Int32),
                Value::Column(Int32Type::from_data(vec![1, 2, 3])),
            ),
            BlockEntry::new(
                DataType::Nullable(Box::new(DataType::Variant)),
                Value::Column(VariantType::from_opt_data(vec![
                    Some(
                        OwnedJsonb::from_str(r#"{"a": 1, "b": {"c": "x"}}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"a": 2, "b": {"c": "y", "d": true}}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"a": 3, "b": {"d": false}}"#)
                            .unwrap()
                            .to_vec(),
                    ), // 'c' is missing here
                ])),
            ),
        ],
        3,
    );

    let result = builder.add_block(&block, &write_settings, &location)?;

    assert!(!result.data.is_empty());
    assert_eq!(result.draft_virtual_block_meta.virtual_col_metas.len(), 3); // Expect a, b.c, b.d

    // Check 'v'['a']
    let meta_a = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['a']",
    )
    .expect("Virtual column ['a'] not found");
    assert_eq!(meta_a.source_column_id, 1);
    assert_eq!(meta_a.name, "['a']");
    assert_eq!(meta_a.data_type, VariantDataType::UInt64); // Inferred as UInt64 because numbers are small positive ints

    // Check 'v'['b']['c']
    let meta_bc = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['b']['c']",
    )
    .expect("Virtual column ['b']['c'] not found");
    assert_eq!(meta_bc.source_column_id, 1);
    assert_eq!(meta_bc.name, "['b']['c']");
    assert_eq!(meta_bc.data_type, VariantDataType::String);

    // Check 'v'['b']['d']
    let meta_bd = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['b']['d']",
    )
    .expect("Virtual column ['b']['d'] not found");
    assert_eq!(meta_bd.source_column_id, 1);
    assert_eq!(meta_bd.name, "['b']['d']");
    assert_eq!(meta_bd.data_type, VariantDataType::Boolean);

    let block = DataBlock::new(
        vec![
            BlockEntry::new(
                DataType::Number(NumberDataType::Int32),
                Value::Column(Int32Type::from_data(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            ),
            BlockEntry::new(
                DataType::Nullable(Box::new(DataType::Variant)),
                Value::Column(VariantType::from_opt_data(vec![
                    Some(
                        OwnedJsonb::from_str(r#"{"id":1, "create": "3/06", "text": "a", "user": {"id": 1}}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"id":2, "create": "3/07", "text": "b", "user": {"id": 3}}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"id":3, "create": "6/07", "text": "c", "user": {"id": 5}}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"id":4, "create": "1/08", "text": "a", "user": {"id": 1}, "replies": 9}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"id":5, "create": "1/10", "text": "b", "user": {"id": 7}, "replies": 3, "geo": {"lat": 1.9}}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"id":6, "create": "1/11", "text": "c", "user": {"id": 1}, "replies": 2, "geo": null}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"id":7, "create": "1/12", "text": "d", "user": {"id": 3}, "replies": 0, "geo": {"lat": 2.7}}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"id":8, "create": "1/13", "text": "x", "user": {"id": 3}, "replies": 1, "geo": {"lat": 3.5}}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                ])),
            ),
        ],
        8,
    );

    let result = builder.add_block(&block, &write_settings, &location)?;

    // Expected columns: id, create, text, user.id, replies, geo.lat
    assert_eq!(result.draft_virtual_block_meta.virtual_col_metas.len(), 6);

    // Check types for good measure
    let meta_id = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['id']",
    )
    .unwrap();
    assert_eq!(meta_id.data_type, VariantDataType::UInt64);
    let meta_create = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['create']",
    )
    .unwrap();
    assert_eq!(meta_create.data_type, VariantDataType::String);
    let meta_text = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['text']",
    )
    .unwrap();
    assert_eq!(meta_text.data_type, VariantDataType::String);
    let meta_user_id = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['user']['id']",
    )
    .unwrap();
    assert_eq!(meta_user_id.data_type, VariantDataType::UInt64);
    let meta_replies = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['replies']",
    )
    .unwrap();
    assert_eq!(meta_replies.data_type, VariantDataType::UInt64);
    let meta_geo_lat = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_col_metas,
        1,
        "['geo']['lat']",
    )
    .unwrap();
    assert_eq!(meta_geo_lat.data_type, VariantDataType::Float64);

    let block = DataBlock::new(
            vec![
            BlockEntry::new(
                DataType::Number(NumberDataType::Int32),
                Value::Column(Int32Type::from_data(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            ),
                BlockEntry::new(
                    DataType::Nullable(Box::new(DataType::Variant)),
                    Value::Column(VariantType::from_opt_data(vec![
                        // Consistent types
                        Some(OwnedJsonb::from_str(r#"{"int_col": 1, "str_col": "a", "bool_col": true, "float_col": 1.1}"#).unwrap().to_vec()),
                        Some(OwnedJsonb::from_str(r#"{"int_col": 2, "str_col": "b", "bool_col": false, "float_col": 2.2}"#).unwrap().to_vec()),
                        // Mixed types -> Variant/Jsonb
                        Some(OwnedJsonb::from_str(r#"{"mixed_col": 10}"#).unwrap().to_vec()),
                        Some(OwnedJsonb::from_str(r#"{"mixed_col": "hello"}"#).unwrap().to_vec()),
                        // Int/UInt coercion
                        Some(OwnedJsonb::from_str(r#"{"num_coerce1": 5, "num_coerce2": 10000000000}"#).unwrap().to_vec()), // UInt64 > i64::MAX
                        Some(OwnedJsonb::from_str(r#"{"num_coerce1": -5, "num_coerce2": 50}"#).unwrap().to_vec()), // Int64
                        // All Nulls (will be discarded later, but type is inferred first if exists)
                        Some(OwnedJsonb::from_str(r#"{"all_null_col": null}"#).unwrap().to_vec()),
                        Some(OwnedJsonb::from_str(r#"{}"#).unwrap().to_vec()),
                    ])),
                ),
            ],
            8, // Number of rows
        );

    let result = builder.add_block(&block, &write_settings, &location)?;

    // all columns should be discarded due to > 70% nulls
    assert!(result.data.is_empty());

    Ok(())
}

fn find_virtual_col<'a>(
    metas: &'a [DraftVirtualColumnMeta],
    source_id: ColumnId,
    name: &str,
) -> Option<&'a DraftVirtualColumnMeta> {
    metas
        .iter()
        .find(|m| m.source_column_id == source_id && m.name == name)
}
