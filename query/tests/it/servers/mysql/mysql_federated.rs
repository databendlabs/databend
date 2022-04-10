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

use common_datablocks::assert_blocks_eq;
use common_exception::Result;
use databend_query::servers::MySQLFederated;

#[test]
fn test_mysql_federated() -> Result<()> {
    let federated = MySQLFederated::create();

    //
    {
        let query = "select 1";
        let result = federated.check(query);
        assert!(result.is_none());
    }

    // select version()
    {
        let query = "select version()";
        let result = federated.check(query);
        assert!(result.is_some());

        if let Some(block) = result {
            assert!(block.is_empty())
        }
    }

    // variables
    {
        let query = "select @@tx_isolation";
        let result = federated.check(query);
        assert!(result.is_some());

        if let Some(block) = result {
            let expect = vec![
                "+----------------+",
                "| @@tx_isolation |",
                "+----------------+",
                "| AUTOCOMMIT     |",
                "+----------------+",
            ];

            assert_blocks_eq(expect, &[block]);
        }
    }

    // complex variables
    {
        let query = "/* mysql-connector-java-8.0.17 (Revision: 16a712ddb3f826a1933ab42b0039f7fb9eebc6ec) */SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout;";
        let result = federated.check(query);
        assert!(result.is_some());

        if let Some(block) = result {
            let expect = vec![
                "+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+-----------+-----------------------+---------------+",
                "| auto_increment_increment | character_set_client | character_set_connection | character_set_results | character_set_server | collation_server | collation_connection | init_connect | interactive_timeout | license | lower_case_table_names | max_allowed_packet | net_write_timeout | performance_schema | sql_mode | system_time_zone | time_zone | transaction_isolation | wait_timeout; |",
                "+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+-----------+-----------------------+---------------+",
                "| 0                        | 0                    | 0                        | 0                     | 0                    | 0                | 0                    | 0            | 0                   | 0       | 0                      | 0                  | 0                 | 0                  | 0        | UTC              | UTC       | 0                     | 0             |",
                "+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+-----------+-----------------------+---------------+",
       ];

            assert_blocks_eq(expect, &[block]);
        }
    }

    Ok(())
}
