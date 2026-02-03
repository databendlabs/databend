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

use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::VariantDataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::VariantType;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::VirtualColumnBuilder;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use jsonb::OwnedJsonb;

#[tokio::test(flavor = "multi_thread")]
async fn test_virtual_column_builder() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(1)?;

    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = fixture.latest_default_table().await?;
    let table_info = table.get_table_info();
    let schema = table_info.meta.schema.clone();

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let write_settings = fuse_table.get_write_settings();
    let location = (
        "_b/h0196236b460676369cfcf6fec0dedefa_v2.parquet".to_string(),
        0,
    ); // Dummy location

    let mut builder = VirtualColumnBuilder::try_create(ctx.clone(), schema.clone()).unwrap();

    let block = DataBlock::new(
        vec![
            (Int32Type::from_data(vec![1, 2, 3])).into(),
            (VariantType::from_opt_data(vec![
                Some(
                    OwnedJsonb::from_str(r#"{"a": 1, "b": {"c": "x"}, "e": [{"f": 100}, 200]}"#)
                        .unwrap()
                        .to_vec(),
                ),
                Some(
                    OwnedJsonb::from_str(
                        r#"{"a": 2, "b": {"c": "y", "d": true}, "e": [{"f": 300}, 400]}"#,
                    )
                    .unwrap()
                    .to_vec(),
                ),
                Some(
                    OwnedJsonb::from_str(r#"{"a": 3, "b": {"d": false}, "e": [{"f": 500}, 600]}"#)
                        .unwrap()
                        .to_vec(),
                ), // 'c' is missing here
            ]))
            .into(),
        ],
        3,
    );

    builder.add_block(&block)?;
    let result = builder.finalize(&write_settings, &location)?;

    assert!(!result.data.is_empty());
    assert_eq!(
        result.draft_virtual_block_meta.virtual_column_metas.len(),
        4
    ); // Expect a, b.c, b.d, e

    // Check v['a']
    let meta_a = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['a']",
    )
    .expect("Virtual column ['a'] not found");
    assert_eq!(meta_a.source_column_id, 1);
    assert_eq!(meta_a.name, "['a']");
    assert_eq!(meta_a.data_type, VariantDataType::UInt64); // Inferred as UInt64 because numbers are small positive ints

    // Check v['b']['c']
    let meta_bc = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['b']['c']",
    )
    .expect("Virtual column ['b']['c'] not found");
    assert_eq!(meta_bc.source_column_id, 1);
    assert_eq!(meta_bc.name, "['b']['c']");
    assert_eq!(meta_bc.data_type, VariantDataType::String);

    // Check v['b']['d']
    let meta_bd = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['b']['d']",
    )
    .expect("Virtual column ['b']['d'] not found");
    assert_eq!(meta_bd.source_column_id, 1);
    assert_eq!(meta_bd.name, "['b']['d']");
    assert_eq!(meta_bd.data_type, VariantDataType::Boolean);

    // Check v['e']
    let meta_bd = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['e']",
    )
    .expect("Virtual column ['e'] not found");
    assert_eq!(meta_bd.source_column_id, 1);
    assert_eq!(meta_bd.name, "['e']");
    assert_eq!(meta_bd.data_type, VariantDataType::Jsonb);

    let block = DataBlock::new(
        vec![
            Int32Type::from_data(vec![1, 2, 3, 4, 5, 6, 7, 8]).into(),
            VariantType::from_opt_data(vec![
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
                ]).into(),
        ],
        8,
    );

    builder.add_block(&block)?;
    let result = builder.finalize(&write_settings, &location)?;

    // Expected columns: id, create, text, user.id, replies, geo.lat, shared_data
    assert_eq!(
        result.draft_virtual_block_meta.virtual_column_metas.len(),
        6
    );

    // Check types for good measure
    let meta_id = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['id']",
    )
    .unwrap();
    assert_eq!(meta_id.data_type, VariantDataType::UInt64);
    let meta_create = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['create']",
    )
    .unwrap();
    assert_eq!(meta_create.data_type, VariantDataType::String);
    let meta_text = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['text']",
    )
    .unwrap();
    assert_eq!(meta_text.data_type, VariantDataType::String);
    let meta_user_id = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['user']['id']",
    )
    .unwrap();
    assert_eq!(meta_user_id.data_type, VariantDataType::UInt64);
    let meta_replies = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['replies']",
    )
    .unwrap();
    assert_eq!(meta_replies.data_type, VariantDataType::UInt64);
    let meta_geo_lat = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['geo']['lat']",
    )
    .unwrap();
    assert_eq!(meta_geo_lat.data_type, VariantDataType::Jsonb);

    let entries = vec![
        Int32Type::from_data(vec![1, 2, 3, 4, 5, 6, 7, 8]).into(),
        VariantType::from_opt_data(vec![
            // Consistent types
            Some(
                OwnedJsonb::from_str(
                    r#"{"int_col": 1, "str_col": "a", "bool_col": true, "float_col": 1.1}"#,
                )
                .unwrap()
                .to_vec(),
            ),
            Some(
                OwnedJsonb::from_str(
                    r#"{"int_col": 2, "str_col": "b", "bool_col": false, "float_col": 2.2}"#,
                )
                .unwrap()
                .to_vec(),
            ),
            // Mixed types -> Variant/Jsonb
            Some(
                OwnedJsonb::from_str(r#"{"mixed_col": 10}"#)
                    .unwrap()
                    .to_vec(),
            ),
            Some(
                OwnedJsonb::from_str(r#"{"mixed_col": "hello"}"#)
                    .unwrap()
                    .to_vec(),
            ),
            // Int/UInt coercion
            Some(
                OwnedJsonb::from_str(r#"{"num_coerce1": 5, "num_coerce2": 10000000000}"#)
                    .unwrap()
                    .to_vec(),
            ), // UInt64 > i64::MAX
            Some(
                OwnedJsonb::from_str(r#"{"num_coerce1": -5, "num_coerce2": 50}"#)
                    .unwrap()
                    .to_vec(),
            ), // Int64
            // All Nulls (will be discarded later, but type is inferred first if exists)
            Some(
                OwnedJsonb::from_str(r#"{"all_null_col": null}"#)
                    .unwrap()
                    .to_vec(),
            ),
            Some(OwnedJsonb::from_str(r#"{}"#).unwrap().to_vec()),
        ])
        .into(),
    ];

    let block = DataBlock::new(
        entries, 8, // Number of rows
    );

    builder.add_block(&block)?;
    let result = builder.finalize(&write_settings, &location)?;

    // all columns should be add to shared_column due to > 70% nulls
    assert!(!result.data.is_empty());
    assert!(
        result
            .draft_virtual_block_meta
            .virtual_column_metas
            .is_empty()
    );

    // Test consecutive blocks with different JSON virtual columns
    // This test verifies that when adding consecutive blocks with completely different
    // JSON structures, both blocks can correctly generate virtual column data
    let mut builder = VirtualColumnBuilder::try_create(ctx, schema).unwrap();

    // First block with one set of JSON fields
    let block1 = DataBlock::new(
        vec![
            Int32Type::from_data(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).into(),
            VariantType::from_opt_data(vec![
                Some(
                    OwnedJsonb::from_str(
                        r#"{"product_id": 101, "price": 19.99, "category": "electronics"}"#,
                    )
                    .unwrap()
                    .to_vec(),
                );
                10
            ])
            .into(),
        ],
        10,
    );

    builder.add_block(&block1)?;
    let result = builder.finalize(&write_settings, &location)?;
    assert!(!result.data.is_empty());

    // We expect to find virtual columns from block1: product_id, price, category
    assert_eq!(
        result.draft_virtual_block_meta.virtual_column_metas.len(),
        3
    );

    // Check that the expected columns from the first block exist
    let expected_columns = vec!["['product_id']", "['price']", "['category']"];
    for expected_column in expected_columns.into_iter() {
        let column_meta = find_virtual_col(
            &result.draft_virtual_block_meta.virtual_column_metas,
            1,
            expected_column,
        );
        assert!(column_meta.is_some());
    }

    // Second block with completely different JSON fields
    let block2 = DataBlock::new(
        vec![
            Int32Type::from_data(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]).into(),
            VariantType::from_opt_data(vec![
                Some(
                    OwnedJsonb::from_str(
                        r#"{"user_id": 201, "email": "user1@example.com", "active": true}"#,
                    )
                    .unwrap()
                    .to_vec(),
                );
                10
            ])
            .into(),
        ],
        10,
    );

    builder.add_block(&block2)?;
    let result = builder.finalize(&write_settings, &location)?;
    assert!(!result.data.is_empty());

    // We expect to find virtual columns from block2: user_id, email, active
    assert_eq!(
        result.draft_virtual_block_meta.virtual_column_metas.len(),
        3
    );

    // Check that the expected columns from the second block exist
    let expected_columns = vec!["['user_id']", "['email']", "['active']"];
    for expected_column in expected_columns.into_iter() {
        let column_meta = find_virtual_col(
            &result.draft_virtual_block_meta.virtual_column_metas,
            1,
            expected_column,
        );
        assert!(column_meta.is_some());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_virtual_column_builder_stream_write() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(1)?;

    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;

    let ctx = fixture.new_query_ctx().await?;

    let table = fixture.latest_default_table().await?;
    let table_info = table.get_table_info();
    let schema = table_info.meta.schema.clone();

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let write_settings = fuse_table.get_write_settings();
    let location = (
        "_b/h0196236b460676369cfcf6fec0dedefa_v2.parquet".to_string(),
        0,
    ); // Dummy location

    let mut builder = VirtualColumnBuilder::try_create(ctx, schema).unwrap();

    // Create blocks with consistent schema across all blocks
    let blocks = vec![
        // Block 1: Simple nested structure
        DataBlock::new(
            vec![
                (Int32Type::from_data(vec![1, 2, 3])).into(),
                (VariantType::from_opt_data(vec![
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 1, "name": "Alice"}, "score": 100}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 2, "name": "Bob"}, "score": 85}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 3, "name": "Charlie"}, "score": 92}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                ]))
                .into(),
            ],
            3,
        ),
        // Block 2: Same structure, different values
        DataBlock::new(
            vec![
                (Int32Type::from_data(vec![4, 5, 6])).into(),
                (VariantType::from_opt_data(vec![
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 4, "name": "Dave"}, "score": 78}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 5, "name": "Eve"}, "score": 95}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 6, "name": "Frank"}, "score": 88}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                ]))
                .into(),
            ],
            3,
        ),
        // Block 3: Same structure with additional fields
        DataBlock::new(
            vec![
                (Int32Type::from_data(vec![7, 8, 9])).into(),
                (VariantType::from_opt_data(vec![
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 7, "name": "Grace", "active": true}, "score": 91, "tags": ["expert"]}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 8, "name": "Heidi", "active": false}, "score": 75, "tags": ["novice"]}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                    Some(
                        OwnedJsonb::from_str(r#"{"user": {"id": 9, "name": "Ivan", "active": true}, "score": 89, "tags": ["intermediate"]}"#)
                            .unwrap()
                            .to_vec(),
                    ),
                ]))
                .into(),
            ],
            3,
        ),
    ];

    // Stream write: add each block to the builder
    for block in &blocks {
        builder.add_block(block)?;
    }

    // Finalize once after adding all blocks
    let result = builder.finalize(&write_settings, &location)?;

    // Verify the result
    assert!(!result.data.is_empty());

    // We expect virtual columns for user.id, user.name, user.active, score, and tags[0]
    assert_eq!(
        result.draft_virtual_block_meta.virtual_column_metas.len(),
        5
    );

    // Check user.id column
    let meta_user_id = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['user']['id']",
    )
    .expect("Virtual column ['user']['id'] not found");
    assert_eq!(meta_user_id.source_column_id, 1);
    assert_eq!(meta_user_id.name, "['user']['id']");
    assert_eq!(meta_user_id.data_type, VariantDataType::UInt64);

    // Check user.name column
    let meta_user_name = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['user']['name']",
    )
    .expect("Virtual column ['user']['name'] not found");
    assert_eq!(meta_user_name.source_column_id, 1);
    assert_eq!(meta_user_name.name, "['user']['name']");
    assert_eq!(meta_user_name.data_type, VariantDataType::String);

    // Check score column
    let meta_score = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['score']",
    )
    .expect("Virtual column ['score'] not found");
    assert_eq!(meta_score.source_column_id, 1);
    assert_eq!(meta_score.name, "['score']");
    assert_eq!(meta_score.data_type, VariantDataType::UInt64);

    // Check user.active column (only present in the third block)
    let meta_user_active = find_virtual_col(
        &result.draft_virtual_block_meta.virtual_column_metas,
        1,
        "['user']['active']",
    )
    .expect("Virtual column ['user']['active'] not found");
    assert_eq!(meta_user_active.source_column_id, 1);
    assert_eq!(meta_user_active.name, "['user']['active']");
    assert_eq!(meta_user_active.data_type, VariantDataType::Boolean);

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
