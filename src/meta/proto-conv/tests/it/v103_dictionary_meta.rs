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

use chrono::Utc;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_types::anyerror::func_name;
use databend_common_expression::TableDataType;
use databend_common_expression as ce;
use maplit::btreemap;

use crate::common;

#[test]
fn test_decode_v103_dictionary_meta() -> anyhow::Result<()> {
    let want = || {
        let name = "my_dict".to_string();
        let source = "MySQL".to_string();
        let mut options: BTreeMap<String, String> = BTreeMap::new();
        options.insert("host".to_string(), "localhost".to_string());
        options.insert("username".to_string(), "root".to_string());
        options.insert("password".to_string(), "1234".to_string());
        options.insert("port".to_string(), "3306".to_string());
        options.insert("database".to_string(), "my_db".to_string());
        let field = TableField {
            name: "my_table".to_string(),
            default_expr: None,
            data_type: TableDataType::String,
            column_id: 1,
            computed_expr: None,
        };
        let fields = vec![field];
        let metadata = BTreeMap::from([("author".to_string(),"example".to_string())]);
        let next_column_id = 1;
        let table_schema = TableSchema {
            fields,
            metadata,
            next_column_id,
        };
        let schema = Arc::new(ce::TableSchema::new_from(
            vec![
                ce::TableField::new("bool", ce::TableDataType::Boolean),
            ],
            btreemap! { s("a") => s("b") },
        ));
        let primary_column_ids = vec![1];
        let comment = "comment_example".to_string();
        let created_on = Utc::now();
        DictionaryMeta {
            name,
            source,
            options,
            schema,
            primary_column_ids,
            comment,
            created_on,
            field_comments: vec!["c".to_string(); 21],
            dropped_on: None,
            updated_on: None
        }
    };
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}