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

use common_base::base::GlobalInstance;
use common_expression::types::DataType;
use common_expression::types::NumberScalar;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_sql::plans::Join;
use common_sql::plans::Scan;
use common_sql::plans::Statistics;
use databend_query::sql::optimizer::SExpr;
use databend_query::sql::planner::plans::JoinType;
use databend_query::sql::planner::Metadata;
use databend_query::sql::plans::BoundColumnRef;
use databend_query::sql::plans::ConstantExpr;
use databend_query::sql::plans::Filter;
use databend_query::sql::plans::FunctionCall;
use databend_query::sql::ColumnBinding;
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
    let thread_name = match std::thread::current().name() {
        None => panic!("thread name is none"),
        Some(thread_name) => thread_name.to_string(),
    };

    GlobalInstance::init_testing(&thread_name);

    let mut metadata = Metadata::default();
    let tab1 = metadata.add_table(
        "catalog".to_string(),
        "database".to_string(),
        Arc::new(DummyTable::new("table".to_string())),
        None,
        false,
    );
    let col1 = metadata.add_base_table_column(
        "col1".to_string(),
        TableDataType::Boolean,
        tab1,
        None,
        None,
    );
    let col2 = metadata.add_base_table_column(
        "col2".to_string(),
        TableDataType::Boolean,
        tab1,
        None,
        None,
    );

    let s_expr = SExpr::create_binary(
        Join {
            right_conditions: vec![
                FunctionCall {
                    span: None,
                    func_name: "plus".to_string(),
                    params: vec![],
                    arguments: vec![
                        BoundColumnRef {
                            span: None,
                            column: ColumnBinding {
                                database_name: None,
                                table_name: None,
                                table_index: None,
                                column_name: "col1".to_string(),
                                index: col1,
                                data_type: Box::new(DataType::Boolean),
                                visibility: Visibility::Visible,
                            },
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
                    column: ColumnBinding {
                        database_name: None,
                        table_name: None,
                        table_index: None,
                        column_name: "col2".to_string(),
                        index: col2,
                        data_type: Box::new(DataType::Boolean),
                        visibility: Visibility::Visible,
                    },
                }
                .into(),
            ],
            non_equi_conditions: vec![],
            join_type: JoinType::Inner,
            marker_index: None,
            from_correlated_subquery: false,
            contain_runtime_filter: false,
        }
        .into(),
        SExpr::create_unary(
            Filter {
                predicates: vec![
                    ConstantExpr {
                        span: None,
                        value: Scalar::Boolean(true),
                    }
                    .into(),
                ],
                is_having: false,
            }
            .into(),
            SExpr::create_leaf(
                Scan {
                    table_index: tab1,
                    columns: Default::default(),
                    push_down_predicates: None,
                    limit: None,
                    order_by: None,
                    prewhere: None,
                    statistics: Statistics {
                        statistics: None,
                        col_stats: Default::default(),
                        is_accurate: false,
                    },
                }
                .into(),
            ),
        ),
        SExpr::create_leaf(
            Scan {
                table_index: tab1,
                columns: Default::default(),
                push_down_predicates: None,
                limit: None,
                order_by: None,
                prewhere: None,
                statistics: Statistics {
                    statistics: None,
                    col_stats: Default::default(),
                    is_accurate: false,
                },
            }
            .into(),
        ),
    );

    let metadata_ref = Arc::new(RwLock::new(metadata));

    let tree = s_expr.to_format_tree(&metadata_ref);
    let result = tree.format_indent().unwrap();
    let expect = "HashJoin: INNER\n    equi conditions: [col2 (#1) eq plus(col1 (#0), 123)]\n    non-equi conditions: []\n    Filter\n        filters: [true]\n        LogicalGet\n            table: catalog.database.table\n            filters: []\n            order by: []\n            limit: NONE\n    LogicalGet\n        table: catalog.database.table\n        filters: []\n        order by: []\n        limit: NONE\n";
    assert_eq!(result.as_str(), expect);
    let pretty_result = tree.format_pretty().unwrap();
    let pretty_expect = "HashJoin: INNER\n├── equi conditions: [col2 (#1) eq plus(col1 (#0), 123)]\n├── non-equi conditions: []\n├── Filter\n│   ├── filters: [true]\n│   └── LogicalGet\n│       ├── table: catalog.database.table\n│       ├── filters: []\n│       ├── order by: []\n│       └── limit: NONE\n└── LogicalGet\n    ├── table: catalog.database.table\n    ├── filters: []\n    ├── order by: []\n    └── limit: NONE\n";
    assert_eq!(pretty_result.as_str(), pretty_expect);
}
