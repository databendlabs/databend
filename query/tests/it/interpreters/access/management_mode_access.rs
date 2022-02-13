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

use common_base::tokio;
use common_exception::Result;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::PlanParser;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_management_mode_access() -> Result<()> {
    struct TestGroup {
        name: &'static str,
        tests: Vec<Test>,
    }

    struct Test {
        name: &'static str,
        query: &'static str,
        is_err: bool,
    }

    let groups: Vec<TestGroup> = vec![
        TestGroup {
            name: "show",
            tests: vec![
                Test {
                    name: "show-databases",
                    query: "SHOW DATABASES",
                    is_err: false,
                },
                Test {
                    name: "show-engines",
                    query: "SHOW ENGINES",
                    is_err: false,
                },
                Test {
                    name: "show-functions",
                    query: "SHOW FUNCTIONS",
                    is_err: false,
                },
                Test {
                    name: "show-grants",
                    query: "SHOW GRANTS",
                    is_err: false,
                },
                Test {
                    name: "show-settings",
                    query: "SHOW SETTINGS",
                    is_err: false,
                },
                Test {
                    name: "show-tables",
                    query: "SHOW TABLES",
                    is_err: false,
                },
                Test {
                    name: "show-users",
                    query: "SHOW USERS",
                    is_err: false,
                },
            ],
        },
        TestGroup {
            name: "database",
            tests: vec![
                Test {
                    name: "db-create-access-passed",
                    query: "CREATE DATABASE db1",
                    is_err: false,
                },
                Test {
                    name: "db-show-access-passed",
                    query: "SHOW CREATE DATABASE db1",
                    is_err: false,
                },
                Test {
                    name: "db-drop-access-passed",
                    query: "DROP DATABASE IF EXISTS db1",
                    is_err: false,
                },
            ],
        },
        TestGroup {
            name: "table",
            tests: vec![
                Test {
                    name: "table-create-access-passed",
                    query: "CREATE TABLE t1(a int)",
                    is_err: false,
                },
                Test {
                    name: "table-desc-access-passed",
                    query: "DESC t1",
                    is_err: false,
                },
                Test {
                    name: "table-show-create-access-passed",
                    query: "SHOW CREATE TABLE t1",
                    is_err: false,
                },
                Test {
                    name: "table-drop-access-passed",
                    query: "DROP TABLE t1",
                    is_err: false,
                },
            ],
        },
        TestGroup {
            name: "stage",
            tests: vec![
                Test {
                    name: "stage-create-access-passed",
                    query: "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter='\n') comments='test'",
                    is_err: false,
                },
                Test {
                    name: "stage-drop-access-passed",
                    query: "DROP STAGE test_stage",
                    is_err: false,
                },
            ],
        },
        TestGroup {
            name: "denied",
            tests: vec![
                Test {
                    name: "table-create-access-passed",
                    query: "CREATE TABLE t1(a int)",
                    is_err: false,
                },
                Test {
                    name: "insert-denied",
                    query: "insert into t1 values(1)",
                    is_err: true,
                },
                Test {
                    name: "select-denied",
                    query: "SELECT * FROM t1",
                    is_err: true,
                },
                Test {
                    name: "show-processlist-denied",
                    query: "SHOW PROCESSLIST",
                    is_err: true,
                },
                Test {
                    name: "show-metrics-denied",
                    query: "SHOW METRICS",
                    is_err: true,
                },
            ],
        },
    ];

    let conf = crate::tests::ConfigBuilder::create()
        .with_management_mode()
        .config();
    let ctx = crate::tests::create_query_context_with_config(conf.clone())?;
    // First to set tenant.
    {
        let plan = PlanParser::parse(ctx.clone(), "SUDO USE TENANT 'test'").await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
        let _ = interpreter.execute(None).await?;
    }

    for group in groups {
        for test in group.tests {
            let plan = PlanParser::parse(ctx.clone(), test.query).await?;
            let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
            let res = interpreter.execute(None).await;
            assert_eq!(
                test.is_err,
                res.is_err(),
                "in test case:{:?}",
                (group.name, test.name)
            );
        }
    }

    Ok(())
}
