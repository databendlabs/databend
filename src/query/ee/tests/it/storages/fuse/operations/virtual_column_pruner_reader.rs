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

use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VirtualColumnField;
use databend_common_catalog::plan::VirtualColumnInfo;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::VariantType;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_fuse::io::VirtualColumnBuilder;
use databend_common_storages_fuse::io::VirtualColumnReader;
use databend_common_storages_fuse::pruning::VirtualColumnPruner;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::*;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::VirtualColumnReadPlan;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use jsonb::OwnedJsonb;
use jsonb::keypath::OwnedKeyPath;
use jsonb::keypath::OwnedKeyPaths;
use jsonb::keypath::parse_key_paths;

#[tokio::test(flavor = "multi_thread")]
async fn test_virtual_column_pruner_reader() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(1)?;
    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;

    let ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let schema = table.get_table_info().meta.schema.clone();
    let source_column_id = schema.column_id_of("v")?;

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let write_settings = fuse_table.get_write_settings();
    let location = ("_b/virtual_column_pruner_reader.parquet".to_string(), 0);

    let json_rows = [
        r#"{"id":1,"create":"3/06","text":"a","user":{"id":1,"name":"alice","info":{"replies":2,"likes":25,"tags":["good","popular"]}}}"#,
        r#"{"id":2,"create":"4/06","text":"b","user":{"id":2,"name":"bob","info":{"replies":12,"tags":["interesting"]}}}"#,
        r#"{"id":3,"create":"4/07","text":"c","user":{"id":3,"name":"tom","info":{"replies":4,"likes":85,"tags":["bad"]},"extra":"new"}}"#,
        r#"{"id":4,"create":"4/08","text":"d","user":{"id":4,"name":"sam","info":{"replies":1,"likes":5,"tags":[]}}}"#,
        r#"{"id":5,"create":"4/09","text":"e","user":{"id":5,"name":"zen","info":{"replies":7}}}"#,
    ];
    let ids = vec![1, 2, 3, 4, 5];
    let variants = json_rows
        .iter()
        .map(|row| Some(OwnedJsonb::from_str(row).unwrap().to_vec()))
        .collect::<Vec<_>>();
    let block = DataBlock::new(
        vec![
            Int32Type::from_data(ids).into(),
            VariantType::from_opt_data(variants).into(),
        ],
        json_rows.len(),
    );

    let mut builder = VirtualColumnBuilder::try_create(ctx.clone(), schema.clone())?;
    builder.add_block(&block)?;
    let state = builder.finalize(&write_settings, &location)?;
    assert!(!state.data.is_empty());

    let dal = fuse_table.get_operator();
    let virtual_location = state.draft_virtual_block_meta.virtual_location.0.clone();
    dal.write(&virtual_location, state.data.clone()).await?;

    let mut column_id = schema.next_column_id();
    let mut virtual_column_fields = Vec::new();
    let mut column_ids = Vec::new();
    for key_path in [
        "{id}",
        "{text}",
        "{user,info}",
        "{user,info,tags,0}",
        "{user,extra}",
    ] {
        let key_paths = parse_key_paths(key_path.as_bytes()).unwrap().to_owned();
        let field = build_virtual_column_field(source_column_id, "v", column_id, key_paths);
        virtual_column_fields.push(field);
        column_ids.push(column_id);
        column_id += 1;
    }

    let mut source_column_ids = HashSet::new();
    source_column_ids.insert(source_column_id);
    let virtual_column_info = VirtualColumnInfo {
        source_column_ids,
        virtual_column_fields,
    };
    let push_down = PushDownInfo {
        virtual_column: Some(virtual_column_info.clone()),
        ..Default::default()
    };

    let virtual_block_meta = VirtualBlockMeta {
        virtual_column_metas: HashMap::new(),
        virtual_column_size: state.draft_virtual_block_meta.virtual_column_size,
        virtual_location: state.draft_virtual_block_meta.virtual_location.clone(),
    };

    let pruner = VirtualColumnPruner::try_create(dal.clone(), &Some(push_down.clone()))?
        .expect("virtual column pruner");
    let virtual_block_meta_index = pruner
        .prune_virtual_columns(&Some(virtual_block_meta))
        .await?
        .expect("virtual block meta index");

    let mut plan_kinds = HashSet::new();
    for plans in virtual_block_meta_index.virtual_column_read_plan.values() {
        for plan in plans {
            collect_plan_kinds(plan, &mut plan_kinds);
        }
    }
    assert!(plan_kinds.contains("Direct"));
    assert!(plan_kinds.contains("FromParent"));
    assert!(plan_kinds.contains("Shared"));
    assert!(plan_kinds.contains("Object"));

    let info_column_id = column_ids[2];
    let tags0_column_id = column_ids[3];
    let extra_column_id = column_ids[4];

    let info_plans = virtual_block_meta_index
        .virtual_column_read_plan
        .get(&info_column_id)
        .expect("user.info plans");
    assert!(
        info_plans
            .iter()
            .any(|plan| matches!(plan, VirtualColumnReadPlan::Object { .. }))
    );

    let tags0_plans = virtual_block_meta_index
        .virtual_column_read_plan
        .get(&tags0_column_id)
        .expect("tags[0] plans");
    assert!(tags0_plans.iter().any(|plan| matches!(
        plan,
        VirtualColumnReadPlan::FromParent { suffix_path, .. } if suffix_path == "{0}"
    )));

    let extra_plans = virtual_block_meta_index
        .virtual_column_read_plan
        .get(&extra_column_id)
        .expect("user.extra plans");
    assert!(
        extra_plans
            .iter()
            .any(|plan| matches!(plan, VirtualColumnReadPlan::Shared { .. }))
    );

    let plan = table
        .read_plan(ctx.clone(), Some(push_down), None, false, true)
        .await?;
    let reader = VirtualColumnReader::try_create(
        ctx.clone(),
        dal.clone(),
        schema.clone(),
        &plan,
        virtual_column_info,
        write_settings.table_compression,
    )?;

    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let read_settings = ReadSettings::from_ctx(&table_ctx)?;
    let virtual_data = reader
        .read_parquet_data_by_merge_io(
            &read_settings,
            &Some(&virtual_block_meta_index),
            block.num_rows(),
        )
        .await
        .expect("virtual block read result");
    let result_block =
        reader.deserialize_virtual_columns(block.clone(), Some(virtual_data), None)?;

    assert_eq!(result_block.num_columns(), block.num_columns() + 5);

    let expected_id = vec![Some("1"), Some("2"), Some("3"), Some("4"), Some("5")];
    let expected_text = vec![
        Some(r#""a""#),
        Some(r#""b""#),
        Some(r#""c""#),
        Some(r#""d""#),
        Some(r#""e""#),
    ];
    let expected_info = vec![
        Some(r#"{"replies":2,"likes":25,"tags":["good","popular"]}"#),
        Some(r#"{"replies":12,"tags":["interesting"]}"#),
        Some(r#"{"replies":4,"likes":85,"tags":["bad"]}"#),
        Some(r#"{"replies":1,"likes":5,"tags":[]}"#),
        Some(r#"{"replies":7}"#),
    ];
    let expected_tags0 = vec![
        Some(r#""good""#),
        Some(r#""interesting""#),
        Some(r#""bad""#),
        None,
        None,
    ];
    let expected_extra = vec![None, None, Some(r#""new""#), None, None];

    assert_variant_column(&result_block.get_by_offset(2).to_column(), &expected_id);
    assert_variant_column(&result_block.get_by_offset(3).to_column(), &expected_text);
    assert_variant_column(&result_block.get_by_offset(4).to_column(), &expected_info);
    assert_variant_column(&result_block.get_by_offset(5).to_column(), &expected_tags0);
    assert_variant_column(&result_block.get_by_offset(6).to_column(), &expected_extra);

    Ok(())
}

fn build_virtual_column_field(
    source_column_id: u32,
    source_name: &str,
    column_id: u32,
    key_paths: OwnedKeyPaths,
) -> VirtualColumnField {
    let name = format_virtual_column_name(source_name, &key_paths);
    VirtualColumnField {
        source_column_id,
        source_name: source_name.to_string(),
        column_id,
        name,
        key_paths,
        cast_func_name: None,
        data_type: Box::new(TableDataType::Variant),
    }
}

fn format_virtual_column_name(source: &str, key_paths: &OwnedKeyPaths) -> String {
    let mut name = source.to_string();
    for path in &key_paths.paths {
        name.push('[');
        match path {
            OwnedKeyPath::Index(idx) => {
                name.push_str(&idx.to_string());
            }
            OwnedKeyPath::Name(key) | OwnedKeyPath::QuotedName(key) => {
                name.push('\'');
                name.push_str(key);
                name.push('\'');
            }
        }
        name.push(']');
    }
    name
}

fn collect_plan_kinds(plan: &VirtualColumnReadPlan, kinds: &mut HashSet<&'static str>) {
    match plan {
        VirtualColumnReadPlan::Direct { .. } => {
            kinds.insert("Direct");
        }
        VirtualColumnReadPlan::FromParent { parent, .. } => {
            kinds.insert("FromParent");
            collect_plan_kinds(parent, kinds);
        }
        VirtualColumnReadPlan::Shared { .. } => {
            kinds.insert("Shared");
        }
        VirtualColumnReadPlan::Object { entries } => {
            kinds.insert("Object");
            for (_, child) in entries {
                collect_plan_kinds(child, kinds);
            }
        }
    }
}

fn assert_variant_column(column: &Column, expected: &[Option<&str>]) {
    assert_eq!(column.len(), expected.len());
    for (idx, expected_json) in expected.iter().enumerate() {
        let scalar = column.index(idx).expect("column row exists");
        match expected_json {
            None => assert!(
                matches!(scalar, ScalarRef::Null),
                "row {idx} expected null, got {scalar:?}"
            ),
            Some(expected_str) => {
                let ScalarRef::Variant(bytes) = scalar else {
                    panic!("row {idx} expected variant, got {scalar:?}");
                };
                let actual = OwnedJsonb::new(bytes.to_vec());
                let expected = OwnedJsonb::from_str(expected_str).unwrap();
                assert_eq!(actual, expected, "row {idx} mismatch");
            }
        }
    }
}
