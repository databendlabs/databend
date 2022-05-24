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

use common_base::infallible::RwLock;
use common_datavalues::BooleanType;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataValue;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_planners::Statistics;
use databend_query::sql::optimizer::SExpr;
use databend_query::sql::plans::BoundColumnRef;
use databend_query::sql::plans::ConstantExpr;
use databend_query::sql::plans::FilterPlan;
use databend_query::sql::plans::FunctionCall;
use databend_query::sql::plans::PhysicalHashJoin;
use databend_query::sql::plans::PhysicalScan;
use databend_query::sql::ColumnBinding;
use databend_query::sql::Metadata;
use databend_query::storages::Table;

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

fn get_dummy_read_source() -> ReadDataSourcePlan {
    ReadDataSourcePlan {
        catalog: "".to_string(),
        source_info: SourceInfo::TableSource(TableInfo {
            ident: TableIdent::new(0, 0),
            desc: "".to_string(),
            name: "".to_string(),
            meta: TableMeta {
                schema: DataSchemaRefExt::create(vec![]),
                ..Default::default()
            },
        }),
        scan_fields: None,
        parts: vec![],
        statistics: Statistics {
            read_rows: 0,
            read_bytes: 0,
            partitions_scanned: 0,
            partitions_total: 0,
            is_exact: false,
        },
        description: "".to_string(),
        tbl_args: None,
        push_downs: None,
    }
}

#[test]
fn test_format() {
    let mut metadata = Metadata::create();
    let col1 = metadata.add_column("col1".to_string(), BooleanType::new_impl(), None);
    let col2 = metadata.add_column("col2".to_string(), BooleanType::new_impl(), None);
    let tab1 = metadata.add_table(
        "catalog".to_string(),
        "database".to_string(),
        Arc::new(DummyTable::new("table".to_string())),
        get_dummy_read_source(),
    );

    let s_expr = SExpr::create_binary(
        PhysicalHashJoin {
            build_keys: vec![FunctionCall {
                func_name: "plus".to_string(),
                arg_types: vec![],
                arguments: vec![
                    BoundColumnRef {
                        column: ColumnBinding {
                            table_name: None,
                            column_name: "col1".to_string(),
                            index: col1,
                            data_type: BooleanType::new_impl(),
                            visible_in_unqualified_wildcard: false,
                        },
                    }
                    .into(),
                    ConstantExpr {
                        value: DataValue::UInt64(123),
                        data_type: BooleanType::new_impl(),
                    }
                    .into(),
                ],
                return_type: BooleanType::new_impl(),
            }
            .into()],
            probe_keys: vec![BoundColumnRef {
                column: ColumnBinding {
                    table_name: None,
                    column_name: "col2".to_string(),
                    index: col2,
                    data_type: BooleanType::new_impl(),
                    visible_in_unqualified_wildcard: false,
                },
            }
            .into()],
        }
        .into(),
        SExpr::create_unary(
            FilterPlan {
                predicates: vec![ConstantExpr {
                    value: DataValue::Boolean(true),
                    data_type: BooleanType::new_impl(),
                }
                .into()],
                is_having: false,
            }
            .into(),
            SExpr::create_leaf(
                PhysicalScan {
                    table_index: tab1,
                    columns: Default::default(),
                }
                .into(),
            ),
        ),
        SExpr::create_leaf(
            PhysicalScan {
                table_index: tab1,
                columns: Default::default(),
            }
            .into(),
        ),
    );

    let metadata_ref = Arc::new(RwLock::new(metadata));

    let tree = s_expr.to_format_tree(&metadata_ref);
    let result = tree.format_indent().unwrap();
    let expect = r#"PhysicalHashJoin: build keys: [plus(col1, 123)], probe keys: [col2]
    Filter: [true]
        PhysicalScan: catalog.database.table
    PhysicalScan: catalog.database.table
"#;
    assert_eq!(result.as_str(), expect);
}
