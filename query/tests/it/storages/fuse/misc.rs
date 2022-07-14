//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use common_datavalues::DataValue;
use databend_query::storages::index::ColumnStatistics;
use serde_json::Value;

// A non-backward compatible change has been introduced by [PR#6067](https://github.com/datafuselabs/databend/pull/6067/files#diff-20030750809780d6492d2fe215a8eb80294aa6a8a5af2cf1bebe17eb740cae35)
// , please also see [issue#6556](https://github.com/datafuselabs/databend/issues/6556)
// therefore, we alias `null_count` with `unset_bits`, to make subsequent versions backward compatible again
#[test]
fn test_issue_6556_column_statistics_ser_de_compatability_null_count_alias()
-> common_exception::Result<()> {
    let col_stats = ColumnStatistics {
        min: DataValue::Null,
        max: DataValue::Null,
        null_count: 0,
        in_memory_size: 0,
    };

    let mut json_value = serde_json::to_value(&col_stats)?;

    // replace "field" null_count with "unset_bits"
    if let Value::Object(ref mut object) = json_value {
        let unset_bits = object.remove("null_count").unwrap();
        object.insert("unset_bits".to_owned(), unset_bits);
    } else {
        panic!("should return json object");
    }

    // test that we can still de-ser it
    let new_json = json_value.to_string();
    let new_col_stats = serde_json::from_str(&new_json)?;
    assert_eq!(col_stats, new_col_stats);
    Ok(())
}
