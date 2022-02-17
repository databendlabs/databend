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

#[test]
fn copy_test() -> Result<()> {
    /*
    let ident = Ident::new("test_csv");
    let v = vec![ident];
    let name = ObjectName(v);

    expect_parse_ok(
        "copy into test_csv from '@my_ext_stage/tutorials/sample.csv' format csv csv_header = 1 field_delimitor = ',';",
        DfStatement::Copy(DfCopy {
            name,
            columns: vec![],
            location: "@my_ext_stage/tutorials/sample.csv".to_string(),
            file_format_options: maplit::hashmap! {
                "csv_header".into() => "1".into(),
                "field_delimitor".into() => ",".into(),
         }
        }),
    )?;

     */

    Ok(())
}

#[test]
fn copy_from_external_test() -> Result<()> {
    /*
    let ident = Ident::new("test_csv");
    let v = vec![ident];
    let name = ObjectName(v);

    expect_parse_ok(
        "copy into mytable
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='$AWS_ACCESS_KEY_ID' aws_secret_key='$AWS_SECRET_ACCESS_KEY')
        encryption=(master_key = '$MASER_KEY')
        file_format = (type = csv field_delimiter = '|' skip_header = 1);",
        DfStatement::Copy(DfCopy {
            name,
            columns: vec![],
            location: "@my_ext_stage/tutorials/sample.csv".to_string(),
            format: "csv".to_string(),
            file_format_options: maplit::hashmap! {
                   "csv_header".into() => "1".into(),
                   "field_delimitor".into() => ",".into(),
            },
        }),
    )?;

     */

    Ok(())
}
