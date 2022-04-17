// Copyright 2021 Datafuse Labs.
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
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::sql::statements::AnalyzableStatement;
use databend_query::sql::DfParser;
use databend_query::sql::DfStatement;
use databend_query::sql::RESERVED_TABLE_OPTION_KEYS;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_statement_create_table_reserved_opt_keys() -> Result<()> {
    let ctx = create_query_context().await?;
    for opt in &*RESERVED_TABLE_OPTION_KEYS {
        let query = format!("CREATE TABLE default.a( c int) {opt}= 1");
        let (mut statements, _) =
            DfParser::parse_sql(query.as_str(), ctx.get_current_session().get_type())?;
        match statements.remove(0) {
            DfStatement::CreateTable(query) => match query.analyze(ctx.clone()).await {
                Err(e) => {
                    assert_eq!(e.code(), ErrorCode::bad_option_code());
                    assert_eq!(e.to_string(), format!("Code: 1022, displayText = the following table options are reserved, please do not specify them in the CREATE TABLE statement: {opt}."));
                }
                _ => panic!("create table with reserved option should return error"),
            },
            _ => panic!("expecting create table statement"),
        }
    }

    Ok(())
}
