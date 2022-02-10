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

use nom::bytes::complete::tag;
use nom::bytes::complete::take_till1;
use nom::character::complete::digit1;
use nom::character::complete::multispace0;
use nom::character::complete::multispace1;
use nom::IResult;

use super::statements::DfCopy;
use super::statements::DfDescribeStage;
use crate::sql::statements::DfAlterUDF;
use crate::sql::statements::DfAlterUser;
use crate::sql::statements::DfCreateDatabase;
use crate::sql::statements::DfCreateStage;
use crate::sql::statements::DfCreateTable;
use crate::sql::statements::DfCreateUDF;
use crate::sql::statements::DfCreateUser;
use crate::sql::statements::DfDescribeTable;
use crate::sql::statements::DfDropDatabase;
use crate::sql::statements::DfDropStage;
use crate::sql::statements::DfDropTable;
use crate::sql::statements::DfDropUDF;
use crate::sql::statements::DfDropUser;
use crate::sql::statements::DfExplain;
use crate::sql::statements::DfGrantStatement;
use crate::sql::statements::DfInsertStatement;
use crate::sql::statements::DfKillStatement;
use crate::sql::statements::DfOptimizeTable;
use crate::sql::statements::DfQueryStatement;
use crate::sql::statements::DfRevokeStatement;
use crate::sql::statements::DfSetVariable;
use crate::sql::statements::DfShowCreateDatabase;
use crate::sql::statements::DfShowCreateTable;
use crate::sql::statements::DfShowDatabases;
use crate::sql::statements::DfShowEngines;
use crate::sql::statements::DfShowFunctions;
use crate::sql::statements::DfShowGrants;
use crate::sql::statements::DfShowMetrics;
use crate::sql::statements::DfShowProcessList;
use crate::sql::statements::DfShowSettings;
use crate::sql::statements::DfShowTables;
use crate::sql::statements::DfShowUsers;
use crate::sql::statements::DfTruncateTable;
use crate::sql::statements::DfUseDatabase;
use crate::sql::statements::DfUseTenant;

/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum DfStatement {
    // ANSI SQL AST node
    Query(Box<DfQueryStatement>),
    Explain(DfExplain),

    // Databases.
    ShowDatabases(DfShowDatabases),
    ShowCreateDatabase(DfShowCreateDatabase),
    CreateDatabase(DfCreateDatabase),
    DropDatabase(DfDropDatabase),
    UseDatabase(DfUseDatabase),
    UseTenant(DfUseTenant),

    // Tables.
    ShowTables(DfShowTables),
    ShowCreateTable(DfShowCreateTable),
    CreateTable(DfCreateTable),
    DescribeTable(DfDescribeTable),
    DescribeStage(DfDescribeStage),
    DropTable(DfDropTable),
    TruncateTable(DfTruncateTable),
    OptimizeTable(DfOptimizeTable),

    // Settings.
    ShowSettings(DfShowSettings),

    // ProcessList
    ShowProcessList(DfShowProcessList),

    // Metrics
    ShowMetrics(DfShowMetrics),

    // Functions
    ShowFunctions(DfShowFunctions),

    // Kill
    KillStatement(DfKillStatement),

    // Set
    SetVariable(DfSetVariable),

    // Insert
    InsertQuery(DfInsertStatement),

    // User
    CreateUser(DfCreateUser),
    AlterUser(DfAlterUser),
    ShowUsers(DfShowUsers),
    DropUser(DfDropUser),

    // Copy
    Copy(DfCopy),

    // Grant
    GrantPrivilege(DfGrantStatement),
    RevokePrivilege(DfRevokeStatement),
    ShowGrants(DfShowGrants),

    // Stage
    CreateStage(DfCreateStage),
    DropStage(DfDropStage),

    // UDF
    CreateUDF(DfCreateUDF),
    DropUDF(DfDropUDF),
    AlterUDF(DfAlterUDF),

    // Engine
    ShowEngines(DfShowEngines),
}

/// Comment hints from SQL.
/// It'll be enabled when using `--comment` in mysql client.
/// Eg: `SELECT * FROM system.number LIMIT 1; -- { ErrorCode 25 }`
#[derive(Debug, Clone, PartialEq)]
pub struct DfHint {
    pub error_code: Option<u16>,
    pub comment: String,
    pub prefix: String,
}

impl DfHint {
    pub fn create_from_comment(comment: &str, prefix: &str) -> Self {
        let error_code = match Self::parse_code(comment) {
            Ok((_, c)) => c,
            Err(_) => None,
        };

        Self {
            error_code,
            comment: comment.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    //  { ErrorCode 25 }
    pub fn parse_code(comment: &str) -> IResult<&str, Option<u16>> {
        let (comment, _) = take_till1(|c| c == '{')(comment)?;
        let (comment, _) = tag("{")(comment)?;
        let (comment, _) = multispace0(comment)?;
        let (comment, _) = tag("ErrorCode")(comment)?;
        let (comment, _) = multispace1(comment)?;
        let (comment, code) = digit1(comment)?;

        let code = code.parse::<u16>().ok();
        Ok((comment, code))
    }
}
