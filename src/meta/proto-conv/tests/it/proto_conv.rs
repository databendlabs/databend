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
use std::vec;

use ce::types::decimal::DecimalSize;
use ce::types::DecimalDataType;
use ce::types::NumberDataType;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression as ce;
use databend_common_expression::types::DataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::IcebergCatalogOption;
use databend_common_meta_app::schema::IcebergRestCatalogOption;
use databend_common_meta_app::schema::IndexType;
use databend_common_meta_app::schema::LockType;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_proto_conv::FromToProto;
use databend_common_proto_conv::Incompatible;
use databend_common_proto_conv::VER;
use maplit::btreemap;
use maplit::btreeset;
use pretty_assertions::assert_eq;

fn s(ss: impl ToString) -> String {
    ss.to_string()
}

fn new_db_meta_share() -> mt::DatabaseMeta {
    mt::DatabaseMeta {
        engine: "44".to_string(),
        engine_options: btreemap! {s("abc") => s("def")},
        options: btreemap! {s("xyz") => s("foo")},
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap(),
        comment: "foo bar".to_string(),
        drop_on: None,
        gc_in_progress: false,
    }
}

fn new_db_meta() -> mt::DatabaseMeta {
    mt::DatabaseMeta {
        engine: "44".to_string(),
        engine_options: btreemap! {s("abc") => s("def")},
        options: btreemap! {s("xyz") => s("foo")},
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap(),
        comment: "foo bar".to_string(),
        drop_on: None,
        gc_in_progress: false,
    }
}

fn new_lvt() -> mt::LeastVisibleTime {
    mt::LeastVisibleTime {
        time: DateTime::<Utc>::from_timestamp(10267, 0).unwrap(),
    }
}

fn new_sequence_meta() -> mt::SequenceMeta {
    mt::SequenceMeta {
        create_on: DateTime::<Utc>::from_timestamp(10267, 0).unwrap(),
        update_on: DateTime::<Utc>::from_timestamp(10267, 0).unwrap(),
        comment: Some("seq".to_string()),
        start: 1,
        step: 1,
        current: 10,
    }
}

fn new_table_meta() -> mt::TableMeta {
    mt::TableMeta {
        schema: Arc::new(ce::TableSchema::new_from(
            vec![
                ce::TableField::new(
                    "nullable",
                    ce::TableDataType::Nullable(Box::new(ce::TableDataType::Number(
                        NumberDataType::Int8,
                    ))),
                )
                .with_default_expr(Some("a + 3".to_string())),
                ce::TableField::new("bool", ce::TableDataType::Boolean),
                ce::TableField::new("int8", ce::TableDataType::Number(NumberDataType::Int8)),
                ce::TableField::new("int16", ce::TableDataType::Number(NumberDataType::Int16)),
                ce::TableField::new("int32", ce::TableDataType::Number(NumberDataType::Int32)),
                ce::TableField::new("int64", ce::TableDataType::Number(NumberDataType::Int64)),
                ce::TableField::new("uint8", ce::TableDataType::Number(NumberDataType::UInt8)),
                ce::TableField::new("uint16", ce::TableDataType::Number(NumberDataType::UInt16)),
                ce::TableField::new("uint32", ce::TableDataType::Number(NumberDataType::UInt32)),
                ce::TableField::new("uint64", ce::TableDataType::Number(NumberDataType::UInt64)),
                ce::TableField::new(
                    "float32",
                    ce::TableDataType::Number(NumberDataType::Float32),
                ),
                ce::TableField::new(
                    "float64",
                    ce::TableDataType::Number(NumberDataType::Float64),
                ),
                ce::TableField::new("date", ce::TableDataType::Date),
                ce::TableField::new("timestamp", ce::TableDataType::Timestamp),
                ce::TableField::new("string", ce::TableDataType::String),
                ce::TableField::new("struct", ce::TableDataType::Tuple {
                    fields_name: vec![s("foo"), s("bar")],
                    fields_type: vec![ce::TableDataType::Boolean, ce::TableDataType::String],
                }),
                ce::TableField::new(
                    "array",
                    ce::TableDataType::Array(Box::new(ce::TableDataType::Boolean)),
                ),
                ce::TableField::new("variant", ce::TableDataType::Variant),
                ce::TableField::new("variant_array", ce::TableDataType::Variant),
                ce::TableField::new("variant_object", ce::TableDataType::Variant),
                // NOTE: It is safe to convert Interval to NULL, because `Interval` is never really used.
                ce::TableField::new("interval", ce::TableDataType::Null),
                ce::TableField::new("bitmap", ce::TableDataType::Bitmap),
                ce::TableField::new("geom", ce::TableDataType::Geometry),
            ],
            btreemap! {s("a") => s("b")},
        )),
        engine: "44".to_string(),
        storage_params: None,
        part_prefix: "".to_string(),
        engine_options: btreemap! {s("abc") => s("def")},
        options: btreemap! {s("xyz") => s("foo")},
        cluster_key: Some("(a + 2, b)".to_string()),
        cluster_key_seq: 0,
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        comment: s("table_comment"),
        field_comments: vec!["c".to_string(); 21],
        virtual_schema: Some(ce::VirtualDataSchema {
            fields: vec![ce::VirtualDataField {
                name: "field_0".to_string(),
                data_types: vec![
                    ce::VariantDataType::Jsonb,
                    ce::VariantDataType::String,
                    ce::VariantDataType::Array(Box::new(ce::VariantDataType::Jsonb)),
                ],
                source_column_id: 19,
                column_id: ce::VIRTUAL_COLUMN_ID_START,
            }],
            metadata: btreemap! {s("a") => s("b")},
            next_column_id: ce::VIRTUAL_COLUMN_ID_START + 1,
            number_of_blocks: 10,
        }),
        drop_on: None,
        statistics: Default::default(),
        shared_by: btreeset! {1},
        column_mask_policy: Some(btreemap! {s("a") => s("b")}),
        indexes: btreemap! {},
    }
}

fn new_index_meta() -> mt::IndexMeta {
    mt::IndexMeta {
        table_id: 7,
        index_type: IndexType::AGGREGATING,
        created_on: Utc.with_ymd_and_hms(2015, 3, 9, 20, 0, 9).unwrap(),
        dropped_on: None,
        updated_on: None,
        original_query: "SELECT a, sum(b) FROM default.t1 WHERE a > 3 GROUP BY b".to_string(),
        query: "SELECT a, SUM(b) FROM default.t1 WHERE a > 3 GROUP BY b".to_string(),
        sync_creation: false,
    }
}

pub(crate) fn new_latest_schema() -> TableSchema {
    let b1 = TableDataType::Tuple {
        fields_name: vec!["b11".to_string(), "b12".to_string()],
        fields_type: vec![TableDataType::Boolean, TableDataType::String],
    };
    let b = TableDataType::Tuple {
        fields_name: vec!["b1".to_string(), "b2".to_string()],
        fields_type: vec![b1, TableDataType::Number(NumberDataType::Int64)],
    };
    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", b),
        TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new(
            "decimal128",
            TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                precision: 18,
                scale: 3,
            })),
        ),
        TableField::new(
            "decimal256",
            TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                precision: 46,
                scale: 6,
            })),
        ),
        TableField::new("empty_map", TableDataType::EmptyMap),
        TableField::new("bitmap", TableDataType::Bitmap),
        TableField::new("geom", TableDataType::Geometry),
    ];
    TableSchema::new(fields)
}

pub(crate) fn new_table_copied_file_info_v6() -> mt::TableCopiedFileInfo {
    mt::TableCopiedFileInfo {
        etag: Some("etag".to_string()),
        content_length: 1024,
        last_modified: Some(Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap()),
    }
}

pub(crate) fn new_empty_proto() -> mt::EmptyProto {
    mt::EmptyProto {}
}

pub(crate) fn new_lock_meta() -> mt::LockMeta {
    mt::LockMeta {
        user: "root".to_string(),
        node: "node".to_string(),
        query_id: "query".to_string(),
        created_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap(),
        acquired_on: Some(Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 15).unwrap()),
        lock_type: LockType::TABLE,
        extra_info: BTreeMap::from([("key".to_string(), "val".to_string())]),
    }
}

fn new_data_mask_meta() -> databend_common_meta_app::data_mask::DatamaskMeta {
    databend_common_meta_app::data_mask::DatamaskMeta {
        args: vec![("a".to_string(), "String".to_string())],
        return_type: "String".to_string(),
        body: "CASE WHEN current_role() IN('ANALYST') THEN VAL ELSE '*********' END".to_string(),
        comment: Some("some comment".to_string()),
        create_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        update_on: Some(Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap()),
    }
}

fn new_table_statistics() -> databend_common_meta_app::schema::TableStatistics {
    databend_common_meta_app::schema::TableStatistics {
        number_of_rows: 100,
        data_bytes: 200,
        compressed_data_bytes: 15,
        index_data_bytes: 20,
        number_of_segments: Some(1),
        number_of_blocks: Some(2),
    }
}

fn new_catalog_meta() -> databend_common_meta_app::schema::CatalogMeta {
    databend_common_meta_app::schema::CatalogMeta {
        catalog_option: CatalogOption::Iceberg(IcebergCatalogOption::Rest(
            IcebergRestCatalogOption {
                uri: "http://127.0.0.1:9900".to_string(),
                warehouse: "databend_has_super_power".to_string(),
                props: Default::default(),
            },
        )),
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
    }
}

fn new_virtual_data_schema() -> ce::VirtualDataSchema {
    ce::VirtualDataSchema {
        fields: vec![ce::VirtualDataField {
            name: "field_0".to_string(),
            data_types: vec![
                ce::VariantDataType::Jsonb,
                ce::VariantDataType::String,
                ce::VariantDataType::Array(Box::new(ce::VariantDataType::Jsonb)),
            ],
            source_column_id: 19,
            column_id: ce::VIRTUAL_COLUMN_ID_START,
        }],
        metadata: btreemap! {s("a") => s("b")},
        next_column_id: ce::VIRTUAL_COLUMN_ID_START + 1,
        number_of_blocks: 10,
    }
}

fn new_udf_server() -> databend_common_meta_app::principal::UDFServer {
    databend_common_meta_app::principal::UDFServer {
        address: "http://127.0.0.1:8888".to_string(),
        handler: "isempty".to_string(),
        headers: BTreeMap::from([
            ("X-Token".to_string(), "abc123".to_string()),
            ("X-Api-Version".to_string(), "11".to_string()),
        ]),
        language: "python".to_string(),
        arg_types: vec![DataType::String],
        return_type: DataType::Boolean,
    }
}

fn new_table_index() -> databend_common_meta_app::schema::TableIndex {
    databend_common_meta_app::schema::TableIndex {
        index_type: TableIndexType::Ngram,
        name: "idx1".to_string(),
        column_ids: vec![1, 2],
        sync_creation: true,
        version: "f10b230153e14f2c84603958d7f864f8".to_string(),
        options: btreemap! {s("tokenizer") => s("chinese")},
    }
}

#[test]
fn test_pb_from_to() -> anyhow::Result<()> {
    let db = new_db_meta();
    let p = db.to_pb()?;
    let got = mt::DatabaseMeta::from_pb(p)?;
    assert_eq!(db, got);

    let tbl = new_table_meta();
    let p = tbl.to_pb()?;
    let got = mt::TableMeta::from_pb(p)?;
    assert_eq!(tbl, got);

    let index = new_index_meta();
    let p = index.to_pb()?;
    let got = mt::IndexMeta::from_pb(p)?;
    assert_eq!(index, got);

    let data_mask_meta = new_data_mask_meta();
    let p = data_mask_meta.to_pb()?;
    let got = databend_common_meta_app::data_mask::DatamaskMeta::from_pb(p)?;
    assert_eq!(data_mask_meta, got);

    let lvt = new_lvt();
    let p = lvt.to_pb()?;
    let got = mt::LeastVisibleTime::from_pb(p)?;
    assert_eq!(lvt, got);

    Ok(())
}

#[test]
fn test_incompatible() -> anyhow::Result<()> {
    let db_meta = new_db_meta();
    let mut p = db_meta.to_pb()?;
    p.ver = VER + 1;
    p.min_reader_ver = VER + 1;

    let res = mt::DatabaseMeta::from_pb(p);
    assert_eq!(
        Incompatible::new(
            format!(
                "executable ver={} is smaller than the min reader version({}) that can read this message",
                VER,
                VER + 1
            )
        ),
        res.unwrap_err()
    );

    let db_meta = new_db_meta();
    let mut p = db_meta.to_pb()?;
    p.ver = 0;
    p.min_reader_ver = 0;

    let res = mt::DatabaseMeta::from_pb(p);
    assert_eq!(
        Incompatible::new(s(
            "message ver=0 is smaller than executable MIN_MSG_VER(1) that this program can read"
        )),
        res.unwrap_err()
    );

    Ok(())
}

#[test]
fn test_build_pb_buf() -> anyhow::Result<()> {
    // build serialized buf of protobuf data, for backward compatibility test with a new version binary.

    // share DatabaseMeta
    {
        let db_meta = new_db_meta_share();
        let p = db_meta.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("db from share:{:?}", buf);
    }

    // DatabaseMeta
    {
        let db_meta = new_db_meta();
        let p = db_meta.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("db:{:?}", buf);
    }

    // TableMeta
    {
        let tbl = new_table_meta();

        let p = tbl.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("table:{:?}", buf);
    }

    // IndexMeta
    {
        let index = new_index_meta();
        let p = index.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("index meta:{buf:?}");
    }

    // TableCopiedFileInfo
    {
        let copied_file = new_table_copied_file_info_v6();
        let p = copied_file.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("copied_file:{:?}", buf);
    }

    // EmptyProto
    {
        let empty_proto = new_empty_proto();
        let p = empty_proto.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("empty_proto:{:?}", buf);
    }

    // LockMeta
    {
        let table_lock_meta = new_lock_meta();
        let p = table_lock_meta.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
    }

    // schema
    {
        let schema = new_latest_schema();
        let p = schema.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("schema:{:?}", buf);
    }

    // data mask
    {
        let data_mask_meta = new_data_mask_meta();
        let p = data_mask_meta.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("data mask:{:?}", buf);
    }

    // table statistics
    {
        let table_statistics = new_table_statistics();
        let p = table_statistics.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("table statistics:{:?}", buf);
    }

    // catalog meta
    {
        let catalog_meta = new_catalog_meta();
        let p = catalog_meta.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("catalog catalog_meta:{:?}", buf);
    }

    // lvt
    {
        let lvt = new_lvt();
        let p = lvt.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("lvt:{:?}", buf);
    }

    // sequence
    {
        let sequence_meta = new_sequence_meta();
        let p = sequence_meta.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("sequence:{:?}", buf);
    }

    // virtual data schema
    {
        let virtual_data_schema = new_virtual_data_schema();
        let p = virtual_data_schema.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("virtual data schema:{:?}", buf);
    }

    // udf server
    {
        let udf_server = new_udf_server();
        let p = udf_server.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("udf server:{:?}", buf);
    }

    // table index
    {
        let table_index = new_table_index();
        let p = table_index.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("table index:{:?}", buf);
    }

    Ok(())
}
