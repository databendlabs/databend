//  Copyright 2021 Datafuse Labs.
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
//

use std::collections::BTreeMap;
use std::convert::TryFrom;

use common_exception::ErrorCode;
use common_exception::Result;

pub const PARTITION_KEYS: &str = "partition_keys";
pub const LOCATION: &str = "location";

// represents hive table schema info
//
// partition_keys,  hive partition keys, such as:  "p_date", "p_hour"
// location,  hive table location, such as: hdfs://namenode:8020/user/hive/warehouse/a.db/b.table/
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HiveTableOptions {
    pub partition_keys: Option<Vec<String>>,
    pub location: Option<String>,
}

impl From<HiveTableOptions> for BTreeMap<String, String> {
    fn from(options: HiveTableOptions) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        if let Some(partition_keys) = options.partition_keys {
            if !partition_keys.is_empty() {
                map.insert(PARTITION_KEYS.to_string(), partition_keys.join(" "));
            }
        }
        options
            .location
            .map(|v| map.insert(LOCATION.to_string(), v));
        map
    }
}

impl TryFrom<&BTreeMap<String, String>> for HiveTableOptions {
    type Error = ErrorCode;
    fn try_from(options: &BTreeMap<String, String>) -> Result<HiveTableOptions> {
        let partition_keys = options.get(PARTITION_KEYS);
        let partition_keys = if let Some(partition_keys) = partition_keys {
            let a: Vec<String> = partition_keys.split(' ').map(str::to_string).collect();
            if !a.is_empty() { Some(a) } else { None }
        } else {
            None
        };

        let location = options
            .get(LOCATION)
            .ok_or_else(|| ErrorCode::UnexpectedError("Hive engine table missing location key"))?
            .clone();
        let options = HiveTableOptions {
            partition_keys,
            location: Some(location),
        };
        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::HiveTableOptions;

    fn do_test_hive_table_options(hive_table_options: HiveTableOptions) {
        let m: BTreeMap<String, String> = hive_table_options.clone().into();
        let options2 = HiveTableOptions::try_from(&m).unwrap();
        assert_eq!(hive_table_options, options2);
    }

    #[test]
    fn test_hive_table_options() {
        let hive_table_options = HiveTableOptions {
            partition_keys: Some(vec!["a".to_string(), "b".to_string()]),
            location: Some("test".to_string()),
        };

        do_test_hive_table_options(hive_table_options);

        let empty = HiveTableOptions {
            partition_keys: None,
            location: Some("test".to_string()),
        };
        do_test_hive_table_options(empty);
    }
}
