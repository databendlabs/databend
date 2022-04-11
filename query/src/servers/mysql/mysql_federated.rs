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

use std::collections::HashMap;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataSchemaRefExt;
use regex::RegexSet;

use crate::servers::mysql::MYSQL_VERSION;

pub struct MySQLFederated {
    mysql_version: String,
    databend_version: String,
}

impl MySQLFederated {
    pub fn create() -> Self {
        MySQLFederated {
            mysql_version: MYSQL_VERSION.to_string(),
            databend_version: crate::configs::DATABEND_COMMIT_VERSION.to_string(),
        }
    }

    // Block is built by constant.
    fn variable_constant_block(name: &str, value: &str) -> Option<DataBlock> {
        Some(DataBlock::create(
            DataSchemaRefExt::create(vec![DataField::new(
                &format!("@@{}", name),
                StringType::arc(),
            )]),
            vec![Series::from_data(vec![value])],
        ))
    }

    // SELECT @@aa, @@bb as cc, @dd...
    // Block is built by the variables.
    fn variable_lazy_block(query: &str) -> Option<DataBlock> {
        let mut default_map = HashMap::new();
        default_map.insert("tx_isolation", "AUTOCOMMIT");
        default_map.insert("tx_transaction_isolation", "AUTOCOMMIT");
        default_map.insert("session.transaction_read_only", "0");
        default_map.insert("time_zone", "UTC");
        default_map.insert("system_time_zone", "UTC");
        // 128M
        default_map.insert("max_allowed_packet", "134217728");
        default_map.insert("interactive_timeout", "31536000");
        default_map.insert("wait_timeout", "31536000");
        default_map.insert("net_write_timeout", "31536000");

        let mut fields = vec![];
        let mut values = vec![];

        let query = query.to_lowercase();
        // select @@aa, @@bb, @@cc as yy, @@dd
        let mut vars: Vec<&str> = query.split("@@").collect();
        if vars.len() > 1 {
            vars.remove(0);
            for var in vars {
                let var = var.trim_end_matches(|c| c == ' ' || c == ',');
                let vars_as: Vec<&str> = var.split(" as ").collect();
                if vars_as.len() == 2 {
                    // @@cc as yy:
                    // var_as is 'yy' as the field name.
                    let var_as = vars_as[1];
                    fields.push(DataField::new(var_as, StringType::arc()));

                    // var is 'cc'.
                    let var = vars_as[0];
                    let value = default_map.get(var).unwrap_or(&"0").to_string();
                    values.push(Series::from_data(vec![value]));
                } else {
                    // @@aa
                    // var is 'aa'
                    fields.push(DataField::new(&format!("@@{}", var), StringType::arc()));

                    let value = default_map.get(var).unwrap_or(&"0").to_string();
                    values.push(Series::from_data(vec![value]));
                }
            }
        }

        Some(DataBlock::create(DataSchemaRefExt::create(fields), values))
    }

    // Lazy check for data block is built in runtime only when the regex matched.
    fn federated_lazy_check(&self, query: &str) -> Option<DataBlock> {
        type LazyBlockFunc = fn(&str) -> Option<DataBlock>;
        let rules_lazy: Vec<(&str, LazyBlockFunc)> = vec![
            ("(?i)^(SELECT @@(.*))", Self::variable_lazy_block),
            (
                "(?i)^(/\\* mysql-connector-java(.*))",
                Self::variable_lazy_block,
            ),
        ];

        let regex_rules = rules_lazy.iter().map(|x| x.0).collect::<Vec<_>>();
        let regex_set = RegexSet::new(&regex_rules).unwrap();
        let matches = regex_set.matches(query);
        for (index, (_regex, func)) in rules_lazy.iter().enumerate() {
            if matches.matched(index) {
                return match func(query) {
                    None => Some(DataBlock::empty()),
                    Some(data_block) => Some(data_block),
                };
            }
        }

        None
    }

    // Light check for the data block is built in compiler time whether the regex matched or not.
    fn federated_light_check(&self, query: &str) -> Option<DataBlock> {
        let rules_light: Vec<(&str, Option<DataBlock>)> = vec![
            (
                "(?i)^(SELECT VERSION())",
                Self::variable_constant_block(
                    "version()",
                    format!("{}-{}", self.mysql_version, self.databend_version.clone()).as_str(),
                ),
            ),
            // Show.
            ("(?i)^(SHOW VARIABLES(.*))", None),
            // Txn.
            ("(?i)^(ROLLBACK(.*))", None),
            ("(?i)^(COMMIT(.*))", None),
            // Set.
            ("(?i)^(SET NAMES(.*))", None),
            ("(?i)^(SET character_set_results(.*))", None),
            ("(?i)^(SET FOREIGN_KEY_CHECKS(.*))", None),
            ("(?i)^(SET AUTOCOMMIT(.*))", None),
            ("(?i)^(SET sql_mode(.*))", None),
            ("(?i)^(SET @@(.*))", None),
            ("(?i)^(SET SESSION TRANSACTION ISOLATION LEVEL(.*))", None),
            // JDBC.
            //("(?i)^(/\\* mysql-connector-java(.*))", None),
            // DBeaver.
            ("(?i)^(SHOW WARNINGS)", None),
            ("(?i)^(/\\* ApplicationName=(.*)SHOW WARNINGS)", None),
            ("(?i)^(/\\* ApplicationName=(.*)SHOW PLUGINS)", None),
            ("(?i)^(/\\* ApplicationName=(.*)SHOW COLLATION)", None),
            ("(?i)^(/\\* ApplicationName=(.*)SHOW CHARSET)", None),
            ("(?i)^(/\\* ApplicationName=(.*)SHOW ENGINES)", None),
            ("(?i)^(/\\* ApplicationName=(.*)SELECT @@(.*))", None),
            ("(?i)^(/\\* ApplicationName=(.*)SHOW @@(.*))", None),
            (
                "(?i)^(/\\* ApplicationName=(.*)SET net_write_timeout(.*))",
                None,
            ),
            (
                "(?i)^(/\\* ApplicationName=(.*)SET SQL_SELECT_LIMIT(.*))",
                None,
            ),
            ("(?i)^(/\\* ApplicationName=(.*)SHOW VARIABLES(.*))", None),
        ];

        let regex_rules = rules_light.iter().map(|x| x.0).collect::<Vec<_>>();
        let regex_set = RegexSet::new(&regex_rules).unwrap();
        let matches = regex_set.matches(query);
        for (index, (_regex, data_block)) in rules_light.iter().enumerate() {
            if matches.matched(index) {
                return match data_block {
                    None => Some(DataBlock::empty()),
                    Some(data_block) => Some(data_block.clone()),
                };
            }
        }

        None
    }

    // Check the query is a federated or driver setup command.
    // Here we fake some values for the command which Databend not supported.
    pub fn check(&self, query: &str) -> Option<DataBlock> {
        let lazy = self.federated_lazy_check(query);
        if lazy.is_none() {
            self.federated_light_check(query)
        } else {
            lazy
        }
    }
}
