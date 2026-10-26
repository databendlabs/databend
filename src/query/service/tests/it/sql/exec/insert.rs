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
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Planner;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_query::interpreters::InterpreterFactory;
use databend_query::interpreters::build_insert_select_physical_plan;
use databend_query::physical_plans::ConstantTableScan;
use databend_query::physical_plans::DistributedInsertSelect;
use databend_query::physical_plans::Exchange;
use databend_query::physical_plans::PaimonWriteRoute;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanCast;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_query::physical_plans::TableWritePrepare;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;
use databend_query::test_kits::execute_pipeline;
use databend_query::test_kits::execute_query;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use futures::TryStreamExt;
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

fn wrap_with_global_shuffle(input: PhysicalPlan) -> PhysicalPlan {
    PhysicalPlan::new(Exchange {
        input,
        kind: FragmentKind::GlobalShuffle,
        keys: vec![RemoteExpr::ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: "partition_key".to_string(),
        }],
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
        TableWritePrepare::from_physical_plan(&route.input).is_some(),
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

#[tokio::test(flavor = "multi_thread")]
async fn test_prepared_global_shuffle_stays_below_insert() -> databend_common_exception::Result<()>
{
    let warehouse = TestWarehouse::new();
    let (_, append_id) = setup_tables(&warehouse.warehouse).await;
    let table = databend_table(&warehouse.warehouse, &append_id).await;
    let (select, bindings) = dummy_select_plan(2);
    let schema = select.output_schema()?;
    let shuffle = wrap_with_global_shuffle(select);

    let plan = build_insert_select_physical_plan(
        shuffle,
        schema.clone(),
        bindings,
        schema,
        table,
        false,
        true,
        TableMetaTimestamps::new(None, Duration::hours(1)),
        true,
    )?;

    let outer = Exchange::from_physical_plan(&plan)
        .expect("distributed insert must synthesize a commit-gather Merge");
    assert_eq!(outer.kind, FragmentKind::Merge);
    let insert = DistributedInsertSelect::from_physical_plan(&outer.input)
        .expect("Merge must wrap DistributedInsertSelect");
    let shuffle = Exchange::from_physical_plan(&insert.input)
        .expect("prepared GlobalShuffle must remain below the insert");
    assert_eq!(shuffle.kind, FragmentKind::GlobalShuffle);

    Ok(())
}

#[test]
fn test_fuse_insert_select_resize() -> anyhow::Result<()> {
    // Table-function binding uses block_in_place. Runtime workers need the same
    // singleton namespace as the test thread when continuations migrate.
    let name = std::thread::current().name().unwrap().to_string();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name(name)
        .enable_all()
        .build()?
        .block_on(check_fuse_insert_select_resize())
}

async fn check_fuse_insert_select_resize() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    for (name, layout) in [
        ("plain", "ROW_PER_BLOCK=1"),
        ("clustered", "CLUSTER BY (n) ROW_PER_BLOCK=1"),
        ("partitioned", "PARTITION BY (n % 2) ROW_PER_BLOCK=16"),
        (
            "partitioned_clustered",
            "PARTITION BY (n % 2) CLUSTER BY (n) ROW_PER_BLOCK=16",
        ),
        (
            "hash_partitioned",
            "PARTITION BY (n % 2) ROW_PER_BLOCK=16 WRITE_DISTRIBUTION_MODE='hash'",
        ),
    ] {
        let ctx = fixture.new_query_ctx().await?;
        execute_command(ctx, &format!("CREATE TABLE {db}.{name}(n UINT64) {layout}")).await?;
    }

    for (table, threads, stream_write, select, expected_writers) in [
        (
            "plain",
            4,
            0,
            "SELECT number FROM numbers(32) LIMIT 16",
            Some(4),
        ),
        ("plain", 1, 0, "SELECT number FROM numbers(4)", Some(1)),
        ("plain", 4, 0, "SELECT number FROM numbers(0)", Some(4)),
        (
            "plain",
            4,
            0,
            "SELECT a.number * 4 + b.number FROM numbers(4) a CROSS JOIN numbers(4) b",
            Some(4),
        ),
        (
            "clustered",
            4,
            0,
            "SELECT number FROM numbers(32) LIMIT 16",
            Some(4),
        ),
        (
            "partitioned",
            4,
            0,
            "SELECT number FROM numbers(32) LIMIT 16",
            Some(4),
        ),
        (
            "partitioned_clustered",
            4,
            0,
            "SELECT number FROM numbers(32) LIMIT 16",
            Some(4),
        ),
        // Preserve the hash-distributed layout stage's width rather than
        // arbitrarily redistributing its lanes before serialization.
        (
            "hash_partitioned",
            4,
            0,
            "SELECT number FROM numbers(32) LIMIT 16",
            None,
        ),
        (
            "plain",
            4,
            1,
            "SELECT number FROM numbers(32) LIMIT 16",
            Some(4),
        ),
        (
            "plain",
            1,
            1,
            "SELECT number FROM numbers(4) LIMIT 4",
            Some(1),
        ),
        ("plain", 4, 1, "SELECT number FROM numbers(0)", Some(4)),
    ] {
        let ctx = fixture.new_query_ctx().await?;
        ctx.get_session_settings().set_max_threads(threads)?;
        ctx.get_session_settings()
            .set_setting("enable_block_stream_write".into(), stream_write.to_string())?;
        ctx.get_session_settings()
            .set_setting("max_execute_time_in_seconds".into(), "15".into())?;
        assert_eq!(ctx.get_settings().get_max_threads()?, threads);
        assert_eq!(
            ctx.get_settings().get_enable_block_stream_write()?,
            stream_write != 0
        );
        let sql = format!("INSERT INTO {db}.{table} {select}");
        let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let pipeline = interpreter.execute2().await?;
        // Construction has finished; no processor tasks are running.
        let count = |name: &str| {
            pipeline
                .main_pipeline
                .graph
                .node_weights()
                .filter(|node| (unsafe { node.proc.name() }) == name)
                .count()
        };
        let writer_name = if stream_write != 0 {
            // All stream-write cases here use the plain Parquet table.
            assert_eq!(
                count("TransformBlockBuilder"),
                1,
                "builder width changed: {sql}"
            );
            assert_eq!(count("TransformSerializeBlock"), 0, "{sql}");
            "TransformBlockWriter"
        } else {
            "TransformSerializeBlock"
        };
        let expected_writers = expected_writers.unwrap_or_else(|| count("TransformPartitionBy"));
        assert!(expected_writers > 0, "missing write layout: {sql}");
        assert_eq!(count(writer_name), expected_writers, "{sql}");
        execute_pipeline(ctx, pipeline).await?;
    }

    for (table, expected) in [
        (
            "plain",
            (0u64..16)
                .chain(0..4)
                .chain(0..16)
                .chain(0..16)
                .chain(0..4)
                .collect::<Vec<_>>(),
        ),
        ("clustered", (0u64..16).collect()),
        ("partitioned", (0u64..16).collect()),
        ("partitioned_clustered", (0u64..16).collect()),
        ("hash_partitioned", (0u64..16).collect()),
    ] {
        let ctx = fixture.new_query_ctx().await?;
        let blocks = execute_query(ctx, &format!("SELECT n FROM {db}.{table}"))
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await?;
        let mut actual = Vec::new();
        for block in blocks {
            for row in 0..block.num_rows() {
                let value = block.get_by_offset(0).index(row).unwrap().to_owned();
                let Scalar::Number(NumberScalar::UInt64(value)) = value else {
                    panic!("unexpected value")
                };
                actual.push(value);
            }
        }
        let mut expected = expected;
        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected, "{table}");
    }
    Ok(())
}
