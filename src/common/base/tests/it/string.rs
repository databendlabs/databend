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

use databend_common_base::base::*;
use databend_common_exception::Result;

#[test]
fn test_progress() -> Result<()> {
    let original_key = "databend/test_user123!!";
    let new_key = escape_for_key(original_key);
    assert_eq!(Ok("databend%2ftest_user123%21%21".to_string()), new_key);
    assert_eq!(
        Ok(original_key.to_string()),
        unescape_for_key(new_key.unwrap().as_str())
    );
    Ok(())
}

#[test]
fn mask_string_test() {
    assert_eq!(mask_string("", 10), "".to_string());
    assert_eq!(mask_string("string", 0), "******".to_string());
    assert_eq!(mask_string("string", 1), "******g".to_string());
    assert_eq!(mask_string("string", 2), "******ng".to_string());
    assert_eq!(mask_string("string", 3), "******ing".to_string());
    assert_eq!(mask_string("string", 20), "string".to_string());
}

#[test]
fn convert_test() {
    assert_eq!(convert_byte_size(0_f64), "0.00 B");
    assert_eq!(convert_byte_size(0.1_f64), "0.10 B");
    assert_eq!(convert_byte_size(1_f64), "1.00 B");
    assert_eq!(convert_byte_size(1023_f64), "1023.00 B");
    assert_eq!(convert_byte_size(1024_f64), "1.00 KiB");
    assert_eq!(convert_byte_size(1229_f64), "1.20 KiB");
    assert_eq!(
        convert_byte_size(1024_f64 * 1024_f64 * 1024_f64),
        "1.00 GiB"
    );

    assert_eq!(convert_number_size(1_f64), "1");
    assert_eq!(convert_number_size(1022_f64), "1.02 thousand");
    assert_eq!(convert_number_size(10222_f64), "10.22 thousand");
}

#[test]
fn test_unescape_string() {
    let cases = vec![
        vec!["a", "a"],
        vec!["abc", "abc"],
        vec!["\\x01", "\x01"],
        vec!["\x01", "\x01"],
        vec!["\t\nabc", "\t\nabc"],
        vec!["\"\t\nabc\"", "\"\t\nabc\""],
        vec!["\"\\t\nabc\"", "\"\t\nabc\""],
        vec!["'\\t\nabc'", "'\t\nabc'"],
        vec!["\\t\\nabc", "\t\nabc"],
        vec!["\\\\", r"\"],
        vec!["\\\\", "\\"],
    ];

    for c in cases {
        assert_eq!(unescape_string(c[0]).unwrap(), c[1]);
    }
}

#[test]
fn test_mask_connection_info() {
    let sql = r#"COPY INTO table1
        FROM 's3://xx/yy
        CONNECTION = (
            ACCESS_KEY_ID = 'aaa' ,
            SECRET_ACCESS_KEY = 'sss' ,
            REGION = 'us-east-2'
        )
        PATTERN = '.*[.]csv'
            FILE_FORMAT = (
            TYPE = CSV,
            FIELD_DELIMITER = '\t',
            RECORD_DELIMITER = '\n',
            SKIP_HEADER = 1
        );"#;

    let actual = mask_connection_info(sql);
    let expect = r#"COPY INTO table1
        FROM 's3://xx/yy
        CONNECTION = (***masked***)
        PATTERN = '.*[.]csv'
            FILE_FORMAT = (
            TYPE = CSV,
            FIELD_DELIMITER = '\t',
            RECORD_DELIMITER = '\n',
            SKIP_HEADER = 1
        );"#;

    assert_eq!(expect, actual);
}
