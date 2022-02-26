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

use common_exception::Result;
use databend_query::sql::statements::DfCopy;
use databend_query::sql::DfStatement;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;

use crate::sql::sql_parser::expect_parse_err;
use crate::sql::sql_parser::expect_parse_ok;

#[test]
fn copy_from_external_test() -> Result<()> {
    struct Test {
        query: &'static str,
        err: &'static str,
        expect: Option<DfCopy>,
    }

    let tests = vec![Test {
        query: "copy into mytable
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1);",
        err: "",
        expect: Some(DfCopy {
            name: ObjectName(vec![Ident::new("mytable")]),
            columns: vec![],
            location: "s3://mybucket/data/files".to_string(),
            credential_options: maplit::hashmap! {
                   "aws_key_id".into() => "my_key_id".into(),
                   "aws_secret_key".into() => "my_secret_key".into(),
            },
            encryption_options: maplit::hashmap! {
                   "master_key".into() => "my_master_key".into(),
            },

            file_format_options: maplit::hashmap! {
                   "type".into() => "csv".into(),
                   "field_delimiter".into() => "|".into(),
                   "skip_header".into() => "1".into(),
            },
            files: vec![],
            on_error: "".to_string(),
            size_limit: "".to_string(),
            validation_mode: "".to_string(),
        }),
    }];

    for test in tests {
        if test.err.is_empty() {
            expect_parse_ok(test.query, DfStatement::Copy(test.expect.unwrap()))?;
        } else {
            expect_parse_err(test.query, test.err.to_string())?;
        }
    }

    Ok(())
}
