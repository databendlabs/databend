// Copyright 2021 Datafuse Labs.
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

use std::fmt::Debug;
use std::sync::Arc;

use common_datavalues as dv;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_meta_types as mt;
use common_meta_types::DatabaseIdent;
use common_meta_types::DatabaseNameIdent;
use common_proto_conv::FromToProto;
use common_proto_conv::Incompatible;
use common_protos::pb;
use maplit::btreemap;

fn s(ss: impl ToString) -> String {
    ss.to_string()
}

fn new_db_info() -> mt::DatabaseInfo {
    mt::DatabaseInfo {
        ident: DatabaseIdent { db_id: 1, seq: 5 },
        name_ident: DatabaseNameIdent {
            tenant: s("t"),
            db_name: s("123"),
        },
        meta: mt::DatabaseMeta {
            engine: "44".to_string(),
            engine_options: btreemap! {s("abc") => s("def")},
            options: btreemap! {s("xyz") => s("foo")},
            created_on: Utc.ymd(2014, 11, 28).and_hms(12, 0, 9),
            updated_on: Utc.ymd(2014, 11, 29).and_hms(12, 0, 9),
            comment: "foo bar".to_string(),
        },
    }
}

fn new_table_info() -> mt::TableInfo {
    mt::TableInfo {
        ident: mt::TableIdent {
            table_id: 5,
            seq: 6,
        },
        desc: "foo".to_string(),
        name: "bar".to_string(),
        meta: mt::TableMeta {
            schema: Arc::new(dv::DataSchema::new_from(
                vec![
                    //
                    dv::DataField::new(
                        "nullable",
                        dv::NullableType::create(dv::Int8Type::default().into()).into(),
                    )
                    .with_default_expr(Some("a + 3".to_string())),
                    dv::DataField::new("bool", dv::BooleanType::default().into()),
                    dv::DataField::new("int8", dv::Int8Type::default().into()),
                    dv::DataField::new("int16", dv::Int16Type::default().into()),
                    dv::DataField::new("int32", dv::Int32Type::default().into()),
                    dv::DataField::new("int64", dv::Int64Type::default().into()),
                    dv::DataField::new("uint8", dv::UInt8Type::default().into()),
                    dv::DataField::new("uint16", dv::UInt16Type::default().into()),
                    dv::DataField::new("uint32", dv::UInt32Type::default().into()),
                    dv::DataField::new("uint64", dv::UInt64Type::default().into()),
                    dv::DataField::new("float32", dv::Float32Type::default().into()),
                    dv::DataField::new("float64", dv::Float64Type::default().into()),
                    dv::DataField::new("date", dv::DateType::default().into()),
                    dv::DataField::new("timestamp", dv::TimestampType::create(5).into()),
                    dv::DataField::new("string", dv::StringType::default().into()),
                    dv::DataField::new(
                        "struct",
                        dv::StructType::create(
                            vec![s("foo"), s("bar")],
                            vec![
                                dv::BooleanType::default().into(),
                                dv::StringType::default().into(),
                            ], //
                        )
                        .into(),
                    ),
                    dv::DataField::new(
                        "array",
                        dv::ArrayType::create(dv::BooleanType::default().into()).into(),
                    ),
                    dv::DataField::new("variant", dv::VariantType::default().into()),
                    dv::DataField::new("variant_array", dv::VariantArrayType::default().into()),
                    dv::DataField::new("variant_object", dv::VariantObjectType::default().into()),
                    dv::DataField::new(
                        "interval",
                        dv::IntervalType::new(dv::IntervalKind::Day).into(),
                    ),
                ],
                btreemap! {s("a") => s("b")},
            )),
            engine: "44".to_string(),
            engine_options: btreemap! {s("abc") => s("def")},
            options: btreemap! {s("xyz") => s("foo")},
            order_keys: Some("(a + 2, b)".to_string()),
            created_on: Utc.ymd(2014, 11, 28).and_hms(12, 0, 9),
            updated_on: Utc.ymd(2014, 11, 29).and_hms(12, 0, 10),
            comment: s("table_comment"),
            statistics: Default::default(),
        },
    }
}

#[test]
fn test_pb_from_to() -> anyhow::Result<()> {
    let db = new_db_info();
    let p = db.to_pb()?;
    let got = mt::DatabaseInfo::from_pb(p)?;
    assert_eq!(db, got);

    let tbl = new_table_info();
    let p = tbl.to_pb()?;
    let got = mt::TableInfo::from_pb(p)?;
    assert_eq!(tbl, got);

    Ok(())
}

#[test]
fn test_incompatible() -> anyhow::Result<()> {
    let db_info = new_db_info();
    let mut p = db_info.to_pb()?;
    p.ver = 2;

    let res = mt::DatabaseInfo::from_pb(p);
    assert_eq!(
        Incompatible {
            reason: s("ver=2 is not compatible with [1, 1]")
        },
        res.unwrap_err()
    );

    Ok(())
}

#[test]
fn test_build_pb_buf() -> anyhow::Result<()> {
    // build serialized buf of protobuf data, for backward compatibility test with a new version binary.

    // DatabaseInfo
    {
        let db_info = new_db_info();
        let p = db_info.to_pb()?;

        let mut buf = vec![];
        common_protos::prost::Message::encode(&p, &mut buf)?;
        println!("{:?}", buf);
    }

    // TableInfo
    {
        let tbl = new_table_info();

        let p = tbl.to_pb()?;

        let mut buf = vec![];
        common_protos::prost::Message::encode(&p, &mut buf)?;
        println!("{:?}", buf);
    }

    Ok(())
}

#[test]
fn test_load_old() -> anyhow::Result<()> {
    // built with `test_build_pb_buf()`

    // DatabaseInfo is loadable
    {
        let db_info_v1: Vec<u8> = vec![
            10, 7, 8, 1, 16, 5, 160, 6, 1, 18, 11, 10, 1, 116, 18, 3, 49, 50, 51, 160, 6, 1, 26,
            93, 34, 10, 10, 3, 120, 121, 122, 18, 3, 102, 111, 111, 42, 2, 52, 52, 50, 10, 10, 3,
            97, 98, 99, 18, 3, 100, 101, 102, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56,
            32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49,
            49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 178, 1, 7, 102,
            111, 111, 32, 98, 97, 114, 160, 6, 1, 160, 6, 1,
        ];

        let p: pb::DatabaseInfo =
            common_protos::prost::Message::decode(db_info_v1.as_slice()).map_err(print_err)?;

        let got = mt::DatabaseInfo::from_pb(p).map_err(print_err)?;

        let want = mt::DatabaseInfo {
            ident: DatabaseIdent { db_id: 1, seq: 5 },
            name_ident: DatabaseNameIdent {
                tenant: s("t"),
                db_name: s("123"),
            },
            meta: mt::DatabaseMeta {
                engine: "44".to_string(),
                engine_options: btreemap! {s("abc") => s("def")},
                options: btreemap! {s("xyz") => s("foo")},
                created_on: Utc.ymd(2014, 11, 28).and_hms(12, 0, 9),
                updated_on: Utc.ymd(2014, 11, 29).and_hms(12, 0, 9),
                comment: "foo bar".to_string(),
            },
        };
        assert_eq!(want, got);
    }

    // TableInfo is loadable
    {
        let tbl_info_v1: Vec<u8> = vec![
            10, 7, 8, 5, 16, 6, 160, 6, 1, 18, 3, 102, 111, 111, 26, 3, 98, 97, 114, 34, 254, 4,
            10, 140, 4, 10, 37, 10, 8, 110, 117, 108, 108, 97, 98, 108, 101, 18, 5, 97, 32, 43, 32,
            51, 26, 15, 10, 10, 10, 5, 26, 0, 160, 6, 1, 160, 6, 1, 160, 6, 1, 160, 6, 1, 10, 16,
            10, 4, 98, 111, 111, 108, 26, 5, 18, 0, 160, 6, 1, 160, 6, 1, 10, 16, 10, 4, 105, 110,
            116, 56, 26, 5, 26, 0, 160, 6, 1, 160, 6, 1, 10, 17, 10, 5, 105, 110, 116, 49, 54, 26,
            5, 34, 0, 160, 6, 1, 160, 6, 1, 10, 17, 10, 5, 105, 110, 116, 51, 50, 26, 5, 42, 0,
            160, 6, 1, 160, 6, 1, 10, 17, 10, 5, 105, 110, 116, 54, 52, 26, 5, 50, 0, 160, 6, 1,
            160, 6, 1, 10, 17, 10, 5, 117, 105, 110, 116, 56, 26, 5, 58, 0, 160, 6, 1, 160, 6, 1,
            10, 18, 10, 6, 117, 105, 110, 116, 49, 54, 26, 5, 66, 0, 160, 6, 1, 160, 6, 1, 10, 18,
            10, 6, 117, 105, 110, 116, 51, 50, 26, 5, 74, 0, 160, 6, 1, 160, 6, 1, 10, 18, 10, 6,
            117, 105, 110, 116, 54, 52, 26, 5, 82, 0, 160, 6, 1, 160, 6, 1, 10, 19, 10, 7, 102,
            108, 111, 97, 116, 51, 50, 26, 5, 90, 0, 160, 6, 1, 160, 6, 1, 10, 19, 10, 7, 102, 108,
            111, 97, 116, 54, 52, 26, 5, 98, 0, 160, 6, 1, 160, 6, 1, 10, 16, 10, 4, 100, 97, 116,
            101, 26, 5, 106, 0, 160, 6, 1, 160, 6, 1, 10, 26, 10, 9, 116, 105, 109, 101, 115, 116,
            97, 109, 112, 26, 10, 114, 5, 8, 5, 160, 6, 1, 160, 6, 1, 160, 6, 1, 10, 18, 10, 6,
            115, 116, 114, 105, 110, 103, 26, 5, 122, 0, 160, 6, 1, 160, 6, 1, 10, 46, 10, 6, 115,
            116, 114, 117, 99, 116, 26, 33, 130, 1, 27, 10, 3, 102, 111, 111, 10, 3, 98, 97, 114,
            18, 5, 18, 0, 160, 6, 1, 18, 5, 122, 0, 160, 6, 1, 160, 6, 1, 160, 6, 1, 160, 6, 1, 10,
            28, 10, 5, 97, 114, 114, 97, 121, 26, 16, 138, 1, 10, 10, 5, 18, 0, 160, 6, 1, 160, 6,
            1, 160, 6, 1, 160, 6, 1, 10, 23, 10, 7, 118, 97, 114, 105, 97, 110, 116, 26, 9, 146, 1,
            3, 160, 6, 1, 160, 6, 1, 160, 6, 1, 10, 29, 10, 13, 118, 97, 114, 105, 97, 110, 116,
            95, 97, 114, 114, 97, 121, 26, 9, 154, 1, 3, 160, 6, 1, 160, 6, 1, 160, 6, 1, 10, 30,
            10, 14, 118, 97, 114, 105, 97, 110, 116, 95, 111, 98, 106, 101, 99, 116, 26, 9, 162, 1,
            3, 160, 6, 1, 160, 6, 1, 160, 6, 1, 10, 26, 10, 8, 105, 110, 116, 101, 114, 118, 97,
            108, 26, 11, 170, 1, 5, 8, 2, 160, 6, 1, 160, 6, 1, 160, 6, 1, 18, 6, 10, 1, 97, 18, 1,
            98, 160, 6, 1, 42, 10, 10, 3, 120, 121, 122, 18, 3, 102, 111, 111, 50, 2, 52, 52, 58,
            10, 10, 3, 97, 98, 99, 18, 3, 100, 101, 102, 74, 10, 40, 97, 32, 43, 32, 50, 44, 32,
            98, 41, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58,
            48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 57, 32, 49, 50,
            58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 178, 1, 13, 116, 97, 98, 108, 101, 95, 99, 111,
            109, 109, 101, 110, 116, 160, 6, 1, 160, 6, 1,
        ];
        let p: pb::TableInfo =
            common_protos::prost::Message::decode(tbl_info_v1.as_slice()).map_err(print_err)?;

        let got = mt::TableInfo::from_pb(p).map_err(print_err)?;

        let want = mt::TableInfo {
            ident: mt::TableIdent {
                table_id: 5,
                seq: 6,
            },
            desc: "foo".to_string(),
            name: "bar".to_string(),
            meta: mt::TableMeta {
                schema: Arc::new(dv::DataSchema::new_from(
                    vec![
                        //
                        dv::DataField::new(
                            "nullable",
                            dv::NullableType::create(dv::Int8Type::default().into()).into(),
                        )
                        .with_default_expr(Some("a + 3".to_string())),
                        dv::DataField::new("bool", dv::BooleanType::default().into()),
                        dv::DataField::new("int8", dv::Int8Type::default().into()),
                        dv::DataField::new("int16", dv::Int16Type::default().into()),
                        dv::DataField::new("int32", dv::Int32Type::default().into()),
                        dv::DataField::new("int64", dv::Int64Type::default().into()),
                        dv::DataField::new("uint8", dv::UInt8Type::default().into()),
                        dv::DataField::new("uint16", dv::UInt16Type::default().into()),
                        dv::DataField::new("uint32", dv::UInt32Type::default().into()),
                        dv::DataField::new("uint64", dv::UInt64Type::default().into()),
                        dv::DataField::new("float32", dv::Float32Type::default().into()),
                        dv::DataField::new("float64", dv::Float64Type::default().into()),
                        dv::DataField::new("date", dv::DateType::default().into()),
                        dv::DataField::new("timestamp", dv::TimestampType::create(5).into()),
                        dv::DataField::new("string", dv::StringType::default().into()),
                        dv::DataField::new(
                            "struct",
                            dv::StructType::create(vec![s("foo"), s("bar")], vec![
                                dv::BooleanType::default().into(),
                                dv::StringType::default().into(),
                            ])
                            .into(),
                        ),
                        dv::DataField::new(
                            "array",
                            dv::ArrayType::create(dv::BooleanType::default().into()).into(),
                        ),
                        dv::DataField::new("variant", dv::VariantType::default().into()),
                        dv::DataField::new("variant_array", dv::VariantArrayType::default().into()),
                        dv::DataField::new(
                            "variant_object",
                            dv::VariantObjectType::default().into(),
                        ),
                        dv::DataField::new(
                            "interval",
                            dv::IntervalType::new(dv::IntervalKind::Day).into(),
                        ),
                    ],
                    btreemap! {s("a") => s("b")},
                )),
                engine: "44".to_string(),
                engine_options: btreemap! {s("abc") => s("def")},
                options: btreemap! {s("xyz") => s("foo")},
                order_keys: Some("(a + 2, b)".to_string()),
                created_on: Utc.ymd(2014, 11, 28).and_hms(12, 0, 9),
                updated_on: Utc.ymd(2014, 11, 29).and_hms(12, 0, 10),
                comment: s("table_comment"),
                statistics: Default::default(),
            },
        };
        assert_eq!(want, got);
    }

    Ok(())
}

fn print_err<T: Debug>(e: T) -> T {
    eprintln!("Error: {:?}", e);
    e
}
