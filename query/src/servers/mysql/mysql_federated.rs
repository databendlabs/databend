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
type LazyBlockFunc = fn(&str) -> Option<DataBlock>;

pub struct MySQLFederated {
    mysql_version: String,
    databend_version: String,
}

impl MySQLFederated {
    pub fn create() -> Self {
        MySQLFederated {
            mysql_version: MYSQL_VERSION.to_string(),
            databend_version: common_configs::DATABEND_COMMIT_VERSION.to_string(),
        }
    }

    // Build block for select @@variable.
    // Format:
    // |@@variable|
    // |value|
    fn select_variable_block(name: &str, value: &str) -> Option<DataBlock> {
        Some(DataBlock::create(
            DataSchemaRefExt::create(vec![DataField::new(
                &format!("@@{}", name),
                StringType::new_impl(),
            )]),
            vec![Series::from_data(vec![value])],
        ))
    }

    // Build block for show variable statement.
    // Format is:
    // |variable_name| Value|
    // | xx          | yy   |
    fn show_variables_block(name: &str, value: &str) -> Option<DataBlock> {
        Some(DataBlock::create(
            DataSchemaRefExt::create(vec![
                DataField::new("Variable_name", StringType::new_impl()),
                DataField::new("Value", StringType::new_impl()),
            ]),
            vec![
                Series::from_data(vec![name]),
                Series::from_data(vec![value]),
            ],
        ))
    }

    fn block_match_rule(
        &self,
        query: &str,
        rules: Vec<(&str, Option<DataBlock>)>,
    ) -> Option<DataBlock> {
        let regex_rules = rules.iter().map(|x| x.0).collect::<Vec<_>>();
        let regex_set = RegexSet::new(&regex_rules).unwrap();
        let matches = regex_set.matches(query);
        for (index, (_regex, data_block)) in rules.iter().enumerate() {
            if matches.matched(index) {
                return match data_block {
                    None => Some(DataBlock::empty()),
                    Some(data_block) => Some(data_block.clone()),
                };
            }
        }

        None
    }

    fn lazy_block_match_rule(
        &self,
        query: &str,
        rules: Vec<(&str, LazyBlockFunc)>,
    ) -> Option<DataBlock> {
        let regex_rules = rules.iter().map(|x| x.0).collect::<Vec<_>>();
        let regex_set = RegexSet::new(&regex_rules).unwrap();
        let matches = regex_set.matches(query);
        for (index, (_regex, func)) in rules.iter().enumerate() {
            if matches.matched(index) {
                return match func(query) {
                    None => Some(DataBlock::empty()),
                    Some(data_block) => Some(data_block),
                };
            }
        }
        None
    }

    // SELECT @@aa, @@bb as cc, @dd...
    // Block is built by the variables.
    fn select_variable_data_block(query: &str) -> Option<DataBlock> {
        let mut default_map = HashMap::new();
        // DBeaver.
        default_map.insert("tx_isolation", "REPEATABLE-READ");
        default_map.insert("session.tx_isolation", "REPEATABLE-READ");
        default_map.insert("transaction_isolation", "REPEATABLE-READ");
        default_map.insert("session.transaction_isolation", "REPEATABLE-READ");
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
                    fields.push(DataField::new(var_as, StringType::new_impl()));

                    // var is 'cc'.
                    let var = vars_as[0];
                    let value = default_map.get(var).unwrap_or(&"0").to_string();
                    values.push(Series::from_data(vec![value]));
                } else {
                    // @@aa
                    // var is 'aa'
                    fields.push(DataField::new(
                        &format!("@@{}", var),
                        StringType::new_impl(),
                    ));

                    let value = default_map.get(var).unwrap_or(&"0").to_string();
                    values.push(Series::from_data(vec![value]));
                }
            }
        }

        Some(DataBlock::create(DataSchemaRefExt::create(fields), values))
    }

    // Check SELECT @@variable, @@variable
    fn federated_select_variable_check(&self, query: &str) -> Option<DataBlock> {
        let rules: Vec<(&str, LazyBlockFunc)> = vec![
            ("(?i)^(SELECT @@(.*))", Self::select_variable_data_block),
            (
                "(?i)^(/\\* mysql-connector-java(.*))",
                Self::select_variable_data_block,
            ),
        ];
        self.lazy_block_match_rule(query, rules)
    }

    // Check SHOW VARIABLES LIKE.
    fn federated_show_variables_check(&self, query: &str) -> Option<DataBlock> {
        let rules: Vec<(&str, Option<DataBlock>)> = vec![
            // sqlalchemy < 1.4.30
            (
                "(?i)^(SHOW VARIABLES LIKE 'sql_mode'(.*))",
                Self::show_variables_block("sql_mode",
                                           "ONLY_FULL_GROUP_BY STRICT_TRANS_TABLES NO_ZERO_IN_DATE NO_ZERO_DATE ERROR_FOR_DIVISION_BY_ZERO NO_ENGINE_SUBSTITUTION"),
            ),
            (
                "(?i)^(SHOW VARIABLES LIKE 'lower_case_table_names'(.*))",
                Self::show_variables_block("lower_case_table_names",
                                           "0"),
            ),
            ("(?i)^(show collation where(.*))", Self::show_variables_block("", "")),
            ("(?i)^(SHOW VARIABLES(.*))", Self::show_variables_block("", "")),
        ];
        self.block_match_rule(query, rules)
    }

    // Check for SET or others query, this is the final check of the federated query.
    fn federated_mixed_check(&self, query: &str) -> Option<DataBlock> {
        let rules: Vec<(&str, Option<DataBlock>)> = vec![
            (
                "(?i)^(SELECT VERSION())",
                Self::select_variable_block(
                    "version()",
                    format!("{}-{}", self.mysql_version, self.databend_version.clone()).as_str(),
                ),
            ),
            // Txn.
            ("(?i)^(ROLLBACK(.*))", None),
            ("(?i)^(COMMIT(.*))", None),
            ("(?i)^(START(.*))", None),
            // Set.
            ("(?i)^(SET NAMES(.*))", None),
            ("(?i)^(SET character_set_results(.*))", None),
            ("(?i)^(SET FOREIGN_KEY_CHECKS(.*))", None),
            ("(?i)^(SET AUTOCOMMIT(.*))", None),
            ("(?i)^(SET SQL_LOG_BIN(.*))", None),
            ("(?i)^(SET sql_mode(.*))", None),
            ("(?i)^(SET @@(.*))", None),
            // mysqldump.
            ("(?i)^(SET SESSION(.*))", None),
            ("(?i)^(SET SQL_QUOTE_SHOW_CREATE(.*))", None),
            ("(?i)^(LOCK TABLES(.*))", None),
            ("(?i)^(UNLOCK TABLES(.*))", None),
            ("(?i)^(SELECT LOGFILE_GROUP_NAME, FILE_NAME, TOTAL_EXTENTS, INITIAL_SIZE, ENGINE, EXTRA FROM INFORMATION_SCHEMA.FILES(.*))", None),
            // mydumper.
            ("(?i)^(SHOW MASTER STATUS)", None),
            ("(?i)^(SHOW ALL SLAVES STATUS)", None),
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

        self.block_match_rule(query, rules)
    }

    // Check the query is a federated or driver setup command.
    // Here we fake some values for the command which Databend not supported.
    pub fn check(&self, query: &str) -> Option<DataBlock> {
        // First to check the select @@variables.
        let select_variable = self.federated_select_variable_check(query);
        if select_variable.is_some() {
            return select_variable;
        }

        // Then to check the show variables like ''.
        let show_variables = self.federated_show_variables_check(query);
        if show_variables.is_some() {
            return show_variables;
        }

        // Last check.
        self.federated_mixed_check(query)
    }
}
