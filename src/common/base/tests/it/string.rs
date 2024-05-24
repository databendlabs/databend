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
use unicode_segmentation::UnicodeSegmentation;

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

#[test]
fn test_short_sql() {
    // Test case 1: SQL query shorter than 128 characters
    let sql1 = "SELECT * FROM users WHERE id = 1;".to_string();
    assert_eq!(short_sql(sql1.clone()), sql1);

    // Test case 2: SQL query longer than 128 characters and starts with "INSERT"
    let long_sql_insert = "INSERT INTO users (id, name, email) VALUES ".to_string()
        + &"(1, 'John Doe', 'john@example.com'), ".repeat(5); // Make sure this creates a string longer than 128 characters
    let expected_length_insert = long_sql_insert.graphemes(true).count().saturating_sub(128);
    let expected_result_insert = {
        let truncated: String = long_sql_insert.graphemes(true).take(128).collect();
        truncated + &format!("...[{} more characters]", expected_length_insert)
    };
    assert_eq!(short_sql(long_sql_insert.clone()), expected_result_insert);

    // Test case 3: SQL query longer than 128 characters but does not start with "INSERT"
    let long_sql_update =
        "UPDATE users SET name = 'John' WHERE id = 1;".to_string() + &"id = 1 OR ".repeat(20); // Make sure this creates a string longer than 128 characters
    assert_eq!(short_sql(long_sql_update.clone()), long_sql_update);

    // Test case 4: Empty SQL query
    let empty_sql = "".to_string();
    assert_eq!(short_sql(empty_sql.clone()), empty_sql);

    // Test case 5: SQL query with leading whitespace
    let sql_with_whitespace =
        "   INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');"
            .to_string();
    let trimmed_sql = sql_with_whitespace.trim_start().to_string();
    assert_eq!(short_sql(sql_with_whitespace.clone()), trimmed_sql);

    // Test case 6: SQL query with multiple emojis to test truncation at an emoji point
    let emoji_sql = "INSERT INTO users (id, name) VALUES (1, 'John Doe 😊😊😊😊😊😊😊😊😊😊');"
        .to_string()
        + &" more text to exceed 128 characters ".repeat(3);
    let expected_emoji_result = {
        let truncated: String = emoji_sql.graphemes(true).take(128).collect();
        let remaining_length = emoji_sql.graphemes(true).count().saturating_sub(128);
        truncated + &format!("...[{} more characters]", remaining_length)
    };
    assert_eq!(short_sql(emoji_sql.clone()), expected_emoji_result);
}
