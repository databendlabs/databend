// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::binder::ColumnBindingBuilder;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::Scan;
use databend_query::sql::optimizer::SExpr;
use databend_query::sql::planner::plans::JoinType;
use databend_query::sql::planner::Metadata;
use databend_query::sql::plans::BoundColumnRef;
use databend_query::sql::plans::ConstantExpr;
use databend_query::sql::plans::Filter;
use databend_query::sql::plans::FunctionCall;
use databend_query::sql::Visibility;
use databend_query::storages::Table;
use parking_lot::RwLock;

struct DummyTable {
    table_info: TableInfo,
}

impl DummyTable {
    pub fn new(table_name: String) -> Self {
        Self {
            table_info: TableInfo {
                ident: TableIdent::new(0, 0),
                desc: "".to_string(),
                name: table_name,
                meta: TableMeta {
                    schema: TableSchemaRefExt::create(vec![]),
                    ..Default::default()
                },
                ..Default::default()
            },
        }
    }
}

impl Table for DummyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

#[test]
fn test_format() {
    let thread_name = std::thread::current()
        .name()
        .map(ToString::to_string)
        .expect("thread should has a name");

    GlobalInstance::init_testing(&thread_name);

    let mut metadata = Metadata::default();
    let tab1 = metadata.add_table(
        "catalog".to_string(),
        "database".to_string(),
        Arc::new(DummyTable::new("table".to_string())),
        None,
        false,
        false,
        false,
    );
    let col1 = metadata.add_base_table_column(
        "col1".to_string(),
        TableDataType::Boolean,
        tab1,
        None,
        None,
        None,
        None,
    );
    let col2 = metadata.add_base_table_column(
        "col2".to_string(),
        TableDataType::Boolean,
        tab1,
        None,
        None,
        None,
        None,
    );

    let s_expr = SExpr::create_binary(
        Arc::new(
            Join {
                right_conditions: vec![
                    FunctionCall {
                        span: None,
                        func_name: "plus".to_string(),
                        params: vec![],
                        arguments: vec![
                            BoundColumnRef {
                                span: None,
                                column: ColumnBindingBuilder::new(
                                    "col1".to_string(),
                                    col1,
                                    Box::new(DataType::Boolean),
                                    Visibility::Visible,
                                )
                                .build(),
                            }
                            .into(),
                            ConstantExpr {
                                span: None,
                                value: Scalar::Number(NumberScalar::UInt64(123u64)),
                            }
                            .into(),
                        ],
                    }
                    .into(),
                ],
                left_conditions: vec![
                    BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "col2".to_string(),
                            col2,
                            Box::new(DataType::Boolean),
                            Visibility::Visible,
                        )
                        .build(),
                    }
                    .into(),
                ],
                non_equi_conditions: vec![],
                join_type: JoinType::Inner,
                marker_index: None,
                from_correlated_subquery: false,
                need_hold_hash_table: false,
                broadcast: false,
            }
            .into(),
        ),
        Arc::new(SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: vec![
                        ConstantExpr {
                            span: None,
                            value: Scalar::Boolean(true),
                        }
                        .into(),
                    ],
                }
                .into(),
            ),
            Arc::new(SExpr::create_leaf(Arc::new(
                Scan {
                    table_index: tab1,
                    ..Default::default()
                }
                .into(),
            ))),
        )),
        Arc::new(SExpr::create_leaf(Arc::new(
            Scan {
                table_index: tab1,
                ..Default::default()
            }
            .into(),
        ))),
    );

    let metadata_ref = Arc::new(RwLock::new(metadata));

    let tree = s_expr.to_format_tree(&metadata_ref);
    let result = tree.format_indent().unwrap();
    let expect = "HashJoin: INNER\n    equi conditions: [eq(col2 (#1), plus(col1 (#0), 123))]\n    non-equi conditions: []\n    Filter\n        filters: [true]\n        LogicalGet\n            table: catalog.database.table\n            filters: []\n            order by: []\n            limit: NONE\n    LogicalGet\n        table: catalog.database.table\n        filters: []\n        order by: []\n        limit: NONE\n";
    assert_eq!(result.as_str(), expect);
    let pretty_result = tree.format_pretty().unwrap();
    let pretty_expect = "HashJoin: INNER\n├── equi conditions: [eq(col2 (#1), plus(col1 (#0), 123))]\n├── non-equi conditions: []\n├── Filter\n│   ├── filters: [true]\n│   └── LogicalGet\n│       ├── table: catalog.database.table\n│       ├── filters: []\n│       ├── order by: []\n│       └── limit: NONE\n└── LogicalGet\n    ├── table: catalog.database.table\n    ├── filters: []\n    ├── order by: []\n    └── limit: NONE\n";
    assert_eq!(pretty_result.as_str(), pretty_expect);
}
