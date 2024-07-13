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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use regex::Regex;

use crate::servers::federated_helper::FederatedHelper;
use crate::servers::federated_helper::LazyBlockFunc;

pub struct MySQLFederated {}

impl MySQLFederated {
    pub fn create() -> Self {
        MySQLFederated {}
    }

    // Build block for select function.
    // Format:
    // |function_name|
    // |value|
    fn select_function_block(name: &str, value: &str) -> Option<(TableSchemaRef, DataBlock)> {
        let schema = TableSchemaRefExt::create(vec![TableField::new(name, TableDataType::String)]);
        let block =
            DataBlock::new_from_columns(vec![StringType::from_data(vec![value.to_string()])]);
        Some((schema, block))
    }

    // Build block for show variable statement.
    // Format is:
    // |variable_name| Value|
    // | xx          | yy   |
    fn show_variables_block(name: &str, value: &str) -> Option<(TableSchemaRef, DataBlock)> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("Variable_name", TableDataType::String),
            TableField::new("Value", TableDataType::String),
        ]);
        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(vec![name.to_string()]),
            StringType::from_data(vec![value.to_string()]),
        ]);
        Some((schema, block))
    }

    // SELECT @@aa, @@bb as cc, @dd...
    // Block is built by the variables.
    fn select_variable_data_block(query: &str) -> Option<(TableSchemaRef, DataBlock)> {
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
                    fields.push(TableField::new(var_as, TableDataType::String));

                    // var is 'cc'.
                    let var = vars_as[0];
                    let value = default_map.get(var).unwrap_or(&"0").to_string();
                    values.push(StringType::from_data(vec![value]));
                } else {
                    // @@aa
                    // var is 'aa'
                    fields.push(TableField::new(
                        &format!("@@{}", var),
                        TableDataType::String,
                    ));

                    let value = default_map.get(var).unwrap_or(&"0").to_string();
                    values.push(StringType::from_data(vec![value]));
                }
            }
        }

        let schema = TableSchemaRefExt::create(fields);
        let block = DataBlock::new_from_columns(values);
        Some((schema, block))
    }

    // Check SELECT @@variable, @@variable
    fn federated_select_variable_check(&self, query: &str) -> Option<(TableSchemaRef, DataBlock)> {
        static SELECT_VARIABLES_LAZY_RULES: LazyLock<Vec<(Regex, LazyBlockFunc)>> =
            LazyLock::new(|| {
                vec![
                    (
                        Regex::new("(?i)^(SELECT @@(.*))").unwrap(),
                        MySQLFederated::select_variable_data_block,
                    ),
                    (
                        Regex::new("(?i)^(/\\* mysql-connector-java(.*))").unwrap(),
                        MySQLFederated::select_variable_data_block,
                    ),
                ]
            });

        FederatedHelper::lazy_block_match_rule(query, &SELECT_VARIABLES_LAZY_RULES)
    }

    // Check SHOW VARIABLES LIKE.
    fn federated_show_variables_check(&self, query: &str) -> Option<(TableSchemaRef, DataBlock)> {
        #![allow(clippy::type_complexity)]
        static SHOW_VARIABLES_RULES: LazyLock<Vec<(Regex, Option<(TableSchemaRef, DataBlock)>)>> =
            LazyLock::new(|| {
                vec![
                    (
                        // sqlalchemy < 1.4.30
                        Regex::new("(?i)^(SHOW VARIABLES LIKE 'sql_mode'(.*))").unwrap(),
                        MySQLFederated::show_variables_block(
                            "sql_mode",
                            "ONLY_FULL_GROUP_BY STRICT_TRANS_TABLES NO_ZERO_IN_DATE NO_ZERO_DATE ERROR_FOR_DIVISION_BY_ZERO NO_ENGINE_SUBSTITUTION",
                        ),
                    ),
                    (
                        Regex::new("(?i)^(SHOW VARIABLES LIKE 'lower_case_table_names'(.*))")
                            .unwrap(),
                        MySQLFederated::show_variables_block("lower_case_table_names", "0"),
                    ),
                    (
                        Regex::new("(?i)^(show collation where(.*))").unwrap(),
                        MySQLFederated::show_variables_block("", ""),
                    ),
                    (
                        Regex::new("(?i)^(SHOW VARIABLES(.*))").unwrap(),
                        MySQLFederated::show_variables_block("", ""),
                    ),
                ]
            });

        FederatedHelper::block_match_rule(query, &SHOW_VARIABLES_RULES)
    }

    // Check for SET or others query, this is the final check of the federated query.
    fn federated_mixed_check(&self, query: &str) -> Option<(TableSchemaRef, DataBlock)> {
        #![allow(clippy::type_complexity)]
        static MIXED_RULES: LazyLock<Vec<(Regex, Option<(TableSchemaRef, DataBlock)>)>> =
            LazyLock::new(|| {
                vec![
            // Txn.
            (Regex::new("(?i)^(START(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET NAMES(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET character_set_results(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET net_write_timeout(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET FOREIGN_KEY_CHECKS(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET AUTOCOMMIT(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET SQL_LOG_BIN(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET sql_mode(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET SQL_SELECT_LIMIT(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET @@(.*))").unwrap(), None),
            // Now databend not support charset and collation
            // https://github.com/datafuselabs/databend/issues/5853
            (Regex::new("(?i)^(SHOW COLLATION)").unwrap(), None),
            (Regex::new("(?i)^(SHOW CHARSET)").unwrap(), None),
            (
                // SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP());
                Regex::new("(?i)^(SELECT TIMEDIFF\\(NOW\\(\\), UTC_TIMESTAMP\\(\\)\\))").unwrap(),
                MySQLFederated::select_function_block("TIMEDIFF(NOW(), UTC_TIMESTAMP())", "00:00:00"),
            ),
            // mysqldump.
            (Regex::new("(?i)^(SET SESSION(.*))").unwrap(), None),
            (Regex::new("(?i)^(SET SQL_QUOTE_SHOW_CREATE(.*))").unwrap(), None),
            (Regex::new("(?i)^(LOCK TABLES(.*))").unwrap(), None),
            (Regex::new("(?i)^(UNLOCK TABLES(.*))").unwrap(), None),
            (
                Regex::new("(?i)^(SELECT LOGFILE_GROUP_NAME, FILE_NAME, TOTAL_EXTENTS, INITIAL_SIZE, ENGINE, EXTRA FROM INFORMATION_SCHEMA.FILES(.*))").unwrap(),
                None,
            ),
            // mydumper.
            (Regex::new("(?i)^(/\\*!80003 SET(.*) \\*/)$").unwrap(), None),
            (Regex::new("(?i)^(SHOW MASTER STATUS)").unwrap(), None),
            (Regex::new("(?i)^(SHOW ALL SLAVES STATUS)").unwrap(), None),
            (Regex::new("(?i)^(LOCK BINLOG FOR BACKUP)").unwrap(), None),
            (Regex::new("(?i)^(LOCK TABLES FOR BACKUP)").unwrap(), None),
            (Regex::new("(?i)^(UNLOCK BINLOG(.*))").unwrap(), None),
            (Regex::new("(?i)^(/\\*!40101 SET(.*) \\*/)$").unwrap(), None),
            // DBeaver.
            (Regex::new("(?i)^(SHOW WARNINGS)").unwrap(), None),
            (Regex::new("(?i)^(/\\* ApplicationName=(.*)SHOW WARNINGS)").unwrap(), None),
            (Regex::new("(?i)^(/\\* ApplicationName=(.*)SHOW PLUGINS)").unwrap(), None),
            (Regex::new("(?i)^(/\\* ApplicationName=(.*)SHOW COLLATION)").unwrap(), None),
            (Regex::new("(?i)^(/\\* ApplicationName=(.*)SHOW CHARSET)").unwrap(), None),
            (Regex::new("(?i)^(/\\* ApplicationName=(.*)SHOW ENGINES)").unwrap(), None),
            (Regex::new("(?i)^(/\\* ApplicationName=(.*)SELECT @@(.*))").unwrap(), None),
            (Regex::new("(?i)^(/\\* ApplicationName=(.*)SHOW @@(.*))").unwrap(), None),
            (
                Regex::new("(?i)^(/\\* ApplicationName=(.*)SET net_write_timeout(.*))").unwrap(),
                None,
            ),
            (
                Regex::new("(?i)^(/\\* ApplicationName=(.*)SET SQL_SELECT_LIMIT(.*))").unwrap(),
                None,
            ),
            (Regex::new("(?i)^(/\\* ApplicationName=(.*)SHOW VARIABLES(.*))").unwrap(), None),
            // mysqldump 5.7.16
            (Regex::new("(?i)^(/\\*!40100 SET(.*) \\*/)$").unwrap(), None),
            (Regex::new("(?i)^(/\\*!40103 SET(.*) \\*/)$").unwrap(), None),
            (Regex::new("(?i)^(/\\*!40111 SET(.*) \\*/)$").unwrap(), None),
            (Regex::new("(?i)^(/\\*!40101 SET(.*) \\*/)$").unwrap(), None),
            (Regex::new("(?i)^(/\\*!40014 SET(.*) \\*/)$").unwrap(), None),
            (Regex::new("(?i)^(/\\*!40000 SET(.*) \\*/)$").unwrap(), None),
            (Regex::new("(?i)^(/\\*!40000 ALTER(.*) \\*/)$").unwrap(), None),
        ]
            });

        FederatedHelper::block_match_rule(query, &MIXED_RULES)
    }

    // Check the query is a federated or driver setup command.
    // Here we fake some values for the command which Databend not supported.
    pub fn check(&self, query: &str) -> Option<(DataSchemaRef, DataBlock)> {
        // First to check the select @@variables.
        let select_variable = self
            .federated_select_variable_check(query)
            .map(|(schema, chunk)| (Arc::new(DataSchema::from(schema)), chunk));
        if select_variable.is_some() {
            return select_variable;
        }

        // Then to check the show variables like ''.
        let show_variables = self
            .federated_show_variables_check(query)
            .map(|(schema, chunk)| (Arc::new(DataSchema::from(schema)), chunk));
        if show_variables.is_some() {
            return show_variables;
        }

        // Last check.
        self.federated_mixed_check(query)
            .map(|(schema, chunk)| (Arc::new(DataSchema::from(schema)), chunk))
    }
}
