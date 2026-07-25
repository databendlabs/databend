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

use chrono::Duration;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_query::interpreters::build_insert_select_physical_plan;
use databend_query::physical_plans::ConstantTableScan;
use databend_query::physical_plans::DistributedInsertSelect;
use databend_query::physical_plans::Exchange;
use databend_query::physical_plans::PaimonWritePrepare;
use databend_query::physical_plans::PaimonWriteRoute;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanCast;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use paimon::Catalog;
use paimon::catalog::Identifier;
use paimon::spec::DataType as PaimonDataType;
use paimon::spec::IntType;
use paimon::spec::Schema;
use paimon::spec::VarCharType;

use crate::storages::paimon::TestWarehouse;
use crate::storages::paimon::databend_table;
use crate::storages::paimon::filesystem_catalog;

async fn setup_tables(warehouse: &str) -> (Identifier, Identifier) {
    let catalog = filesystem_catalog(warehouse);
    catalog
        .create_database("db", false, HashMap::new())
        .await
        .expect("create db");

    let pk_schema = Schema::builder()
        .column("part", PaimonDataType::Int(IntType::new()))
        .column("id", PaimonDataType::Int(IntType::new()))
        .column("name", PaimonDataType::VarChar(VarCharType::string_type()))
        .partition_keys(["part"])
        .primary_key(["part", "id"])
        .option("bucket", "4")
        .build()
        .expect("pk schema");
    let pk_id = Identifier::new("db", "pk_part_t");
    catalog
        .create_table(&pk_id, pk_schema, false)
        .await
        .expect("create pk part table");

    let append_schema = Schema::builder()
        .column("id", PaimonDataType::Int(IntType::new()))
        .column("name", PaimonDataType::VarChar(VarCharType::string_type()))
        .build()
        .expect("append schema");
    let append_id = Identifier::new("db", "append_t");
    catalog
        .create_table(&append_id, append_schema, false)
        .await
        .expect("create append table");

    (pk_id, append_id)
}

fn dummy_select_plan(num_fields: usize) -> (PhysicalPlan, Vec<databend_common_sql::ColumnBinding>) {
    let fields: Vec<_> = (0..num_fields)
        .map(|i| DataField::new(&i.to_string(), DataType::Number(NumberDataType::Int32)))
        .collect();
    // Last field may be String for name columns — keep Int32 for plan-structure test.
    let output_schema = DataSchemaRefExt::create(fields);
    let bindings = (0..num_fields)
        .map(|i| {
            ColumnBindingBuilder::new(
                format!("c{i}"),
                Symbol::from_field_index(i),
                Box::new(DataType::Number(NumberDataType::Int32)),
                Visibility::Visible,
            )
            .build()
        })
        .collect();
    let plan = PhysicalPlan::new(ConstantTableScan {
        values: vec![],
        num_rows: 0,
        output_schema,
        meta: PhysicalPlanMeta::new("ConstantTableScan"),
    });
    (plan, bindings)
}

fn format_plan(plan: &PhysicalPlan) -> String {
    // Debug includes node names and FragmentKind::GlobalShuffle without requiring
    // a fully populated planner Metadata (Exchange pretty-format needs column entries).
    format!("{plan:?}")
}

fn wrap_with_merge_exchange(input: PhysicalPlan) -> PhysicalPlan {
    PhysicalPlan::new(Exchange {
        input,
        kind: FragmentKind::Merge,
        keys: vec![],
        allow_adjust_parallelism: true,
        ignore_exchange: false,
        meta: PhysicalPlanMeta::new("Exchange"),
    })
}

fn assert_pk_write_route_shape(plan: &PhysicalPlan) {
    let plan_text = format_plan(plan);
    assert!(
        plan_text.contains("PaimonWriteRoute"),
        "pk plan missing PaimonWriteRoute:\n{plan_text}"
    );
    assert!(
        plan_text.contains("GlobalShuffle"),
        "pk plan missing GlobalShuffle:\n{plan_text}"
    );
    assert!(
        plan_text.contains("Merge"),
        "pk plan missing Merge Exchange for commit gather:\n{plan_text}"
    );
    assert!(
        plan_text.contains("DistributedInsertSelect"),
        "pk plan missing DistributedInsertSelect:\n{plan_text}"
    );

    // Merge → DistributedInsertSelect → GlobalShuffle → PaimonWriteRoute
    let outer = Exchange::from_physical_plan(plan).expect("pk plan must start with Merge Exchange");
    assert_eq!(outer.kind, FragmentKind::Merge);
    let insert = DistributedInsertSelect::from_physical_plan(&outer.input)
        .expect("Merge must wrap DistributedInsertSelect");
    let shuffle = Exchange::from_physical_plan(&insert.input)
        .expect("insert input must be GlobalShuffle Exchange");
    assert_eq!(shuffle.kind, FragmentKind::GlobalShuffle);
    assert!(
        !shuffle.allow_adjust_parallelism,
        "GlobalShuffle must keep allow_adjust_parallelism=false"
    );
    let route = PaimonWriteRoute::from_physical_plan(&shuffle.input)
        .expect("GlobalShuffle must wrap PaimonWriteRoute");
    assert!(
        PaimonWritePrepare::from_physical_plan(&route.input).is_some(),
        "route input must be cast/fill/reorder prepared"
    );
    assert!(
        insert.input_prepared,
        "insert must not prepare routed data again"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_paimon_write_route_plan() -> databend_common_exception::Result<()> {
    let warehouse = TestWarehouse::new();
    let (pk_id, append_id) = setup_tables(&warehouse.warehouse).await;

    // Local / VALUES path: no top Merge on select → synthesize outer Merge.
    let pk_table = databend_table(&warehouse.warehouse, &pk_id).await;
    let (pk_select, pk_bindings) = dummy_select_plan(3);
    let pk_schema = pk_select.output_schema()?;
    let pk_plan = build_insert_select_physical_plan(
        pk_select,
        pk_schema.clone(),
        pk_bindings,
        pk_schema,
        pk_table,
        false,
        TableMetaTimestamps::new(None, Duration::hours(1)),
        true,
    )?;
    assert_pk_write_route_shape(&pk_plan);

    // A single node keeps the local GlobalShuffle but must not synthesize a
    // self Merge, because the dataflow diagram intentionally has no self edge.
    let pk_table = databend_table(&warehouse.warehouse, &pk_id).await;
    let (pk_select, pk_bindings) = dummy_select_plan(3);
    let pk_schema = pk_select.output_schema()?;
    let local_pk_plan = build_insert_select_physical_plan(
        pk_select,
        pk_schema.clone(),
        pk_bindings,
        pk_schema,
        pk_table,
        false,
        TableMetaTimestamps::new(None, Duration::hours(1)),
        false,
    )?;
    let local_insert = DistributedInsertSelect::from_physical_plan(&local_pk_plan)
        .expect("single-node PK plan must start with DistributedInsertSelect");
    assert!(
        Exchange::from_physical_plan(&local_insert.input)
            .is_some_and(|e| e.kind == FragmentKind::GlobalShuffle),
        "single-node PK insert must retain local GlobalShuffle"
    );

    let append_table = databend_table(&warehouse.warehouse, &append_id).await;
    let (append_select, append_bindings) = dummy_select_plan(2);
    let append_schema = append_select.output_schema()?;
    let append_plan = build_insert_select_physical_plan(
        append_select,
        append_schema.clone(),
        append_bindings,
        append_schema,
        append_table,
        false,
        TableMetaTimestamps::new(None, Duration::hours(1)),
        true,
    )?;
    let append_text = format_plan(&append_plan);
    assert!(
        !append_text.contains("PaimonWriteRoute"),
        "append plan must not contain PaimonWriteRoute:\n{append_text}"
    );
    assert!(
        append_text.contains("DistributedInsertSelect"),
        "append plan missing DistributedInsertSelect:\n{append_text}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_paimon_write_route_plan_preserves_select_merge()
-> databend_common_exception::Result<()> {
    let warehouse = TestWarehouse::new();
    let (pk_id, append_id) = setup_tables(&warehouse.warehouse).await;

    // SELECT path: select already has top Merge → must keep it via exchange.derive.
    let pk_table = databend_table(&warehouse.warehouse, &pk_id).await;
    let (pk_select, pk_bindings) = dummy_select_plan(3);
    let pk_schema = pk_select.output_schema()?;
    let pk_select_with_merge = wrap_with_merge_exchange(pk_select);
    assert!(
        Exchange::from_physical_plan(&pk_select_with_merge)
            .is_some_and(|e| e.kind == FragmentKind::Merge),
        "precondition: select must start with Merge Exchange"
    );

    let pk_plan = build_insert_select_physical_plan(
        pk_select_with_merge,
        pk_schema.clone(),
        pk_bindings,
        pk_schema,
        pk_table,
        false,
        TableMetaTimestamps::new(None, Duration::hours(1)),
        true,
    )?;
    assert_pk_write_route_shape(&pk_plan);

    // Append with top Merge: keep Merge, still no PaimonWriteRoute / GlobalShuffle.
    let append_table = databend_table(&warehouse.warehouse, &append_id).await;
    let (append_select, append_bindings) = dummy_select_plan(2);
    let append_schema = append_select.output_schema()?;
    let append_plan = build_insert_select_physical_plan(
        wrap_with_merge_exchange(append_select),
        append_schema.clone(),
        append_bindings,
        append_schema,
        append_table,
        false,
        TableMetaTimestamps::new(None, Duration::hours(1)),
        true,
    )?;
    let append_text = format_plan(&append_plan);
    assert!(
        !append_text.contains("PaimonWriteRoute"),
        "append plan must not contain PaimonWriteRoute:\n{append_text}"
    );
    assert!(
        !append_text.contains("GlobalShuffle"),
        "append plan must not force GlobalShuffle:\n{append_text}"
    );
    let outer = Exchange::from_physical_plan(&append_plan)
        .expect("append with select Merge must keep outer Exchange");
    assert_eq!(outer.kind, FragmentKind::Merge);
    assert!(
        DistributedInsertSelect::from_physical_plan(&outer.input).is_some(),
        "append Merge must wrap DistributedInsertSelect"
    );

    Ok(())
}
