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

use common_datavalues::BooleanType;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataValue;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use databend_query::sql::optimizer::SExpr;
use databend_query::sql::planner::plans::JoinType;
use databend_query::sql::plans::BoundColumnRef;
use databend_query::sql::plans::ConstantExpr;
use databend_query::sql::plans::Filter;
use databend_query::sql::plans::FunctionCall;
use databend_query::sql::plans::PhysicalHashJoin;
use databend_query::sql::plans::PhysicalScan;
use databend_query::sql::ColumnBinding;
use databend_query::sql::Metadata;
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
                    schema: DataSchemaRefExt::create(vec![]),
                    ..Default::default()
                },
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
    let mut metadata = Metadata::create();
    let col1 = metadata.add_column("col1".to_string(), BooleanType::new_impl(), None, None);
    let col2 = metadata.add_column("col2".to_string(), BooleanType::new_impl(), None, None);
    let tab1 = metadata.add_table(
        "catalog".to_string(),
        "database".to_string(),
        Arc::new(DummyTable::new("table".to_string())),
    );

    let s_expr = SExpr::create_binary(
        PhysicalHashJoin {
            build_keys: vec![
                FunctionCall {
                    func_name: "plus".to_string(),
                    arg_types: vec![],
                    arguments: vec![
                        BoundColumnRef {
                            column: ColumnBinding {
                                database_name: None,
                                table_name: None,
                                column_name: "col1".to_string(),
                                index: col1,
                                data_type: Box::new(BooleanType::new_impl()),
                                visible_in_unqualified_wildcard: false,
                            },
                        }
                        .into(),
                        ConstantExpr {
                            value: DataValue::UInt64(123),
                            data_type: Box::new(BooleanType::new_impl()),
                        }
                        .into(),
                    ],
                    return_type: Box::new(BooleanType::new_impl()),
                }
                .into(),
            ],
            probe_keys: vec![
                BoundColumnRef {
                    column: ColumnBinding {
                        database_name: None,
                        table_name: None,
                        column_name: "col2".to_string(),
                        index: col2,
                        data_type: Box::new(BooleanType::new_impl()),
                        visible_in_unqualified_wildcard: false,
                    },
                }
                .into(),
            ],
            other_conditions: vec![],
            join_type: JoinType::Inner,
            marker_index: None,
            from_correlated_subquery: false,
        }
        .into(),
        SExpr::create_unary(
            Filter {
                predicates: vec![
                    ConstantExpr {
                        value: DataValue::Boolean(true),
                        data_type: Box::new(BooleanType::new_impl()),
                    }
                    .into(),
                ],
                is_having: false,
            }
            .into(),
            SExpr::create_leaf(
                PhysicalScan {
                    table_index: tab1,
                    columns: Default::default(),
                    push_down_predicates: None,
                    limit: None,
                    order_by: None,
                    prewhere: None,
                }
                .into(),
            ),
        ),
        SExpr::create_leaf(
            PhysicalScan {
                table_index: tab1,
                columns: Default::default(),
                push_down_predicates: None,
                limit: None,
                order_by: None,
                prewhere: None,
            }
            .into(),
        ),
    );

    let metadata_ref = Arc::new(RwLock::new(metadata));

    let tree = s_expr.to_format_tree(&metadata_ref);
    let result = tree.format_indent().unwrap();
    let expect = "HashJoin: INNER\n    build keys: [plus(col1 (#0), 123)]\n    probe keys: [col2 (#1)]\n    other filters: []\n    Filter\n        filters: [true]\n        PhysicalScan\n            table: catalog.database.table\n            filters: []\n            order by: []\n            limit: NONE\n    PhysicalScan\n        table: catalog.database.table\n        filters: []\n        order by: []\n        limit: NONE\n";
    assert_eq!(result.as_str(), expect);
    let pretty_result = tree.format_pretty().unwrap();
    let pretty_expect = "HashJoin: INNER\n├── build keys: [plus(col1 (#0), 123)]\n├── probe keys: [col2 (#1)]\n├── other filters: []\n├── Filter\n│   ├── filters: [true]\n│   └── PhysicalScan\n│       ├── table: catalog.database.table\n│       ├── filters: []\n│       ├── order by: []\n│       └── limit: NONE\n└── PhysicalScan\n    ├── table: catalog.database.table\n    ├── filters: []\n    ├── order by: []\n    └── limit: NONE\n";
    assert_eq!(pretty_result.as_str(), pretty_expect);
}
