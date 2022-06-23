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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::write_space_seperated_map;
use crate::ast::Identifier;
use crate::ast::Query;

/// CopyStmt is the parsed statement of `COPY`.
///
/// ## Examples
///
/// ```sql
/// COPY INTO table from s3://bucket/path/to/x.csv
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CopyStmt<'a> {
    pub src: CopyUnit<'a>,
    pub dst: CopyUnit<'a>,
    pub files: Vec<String>,
    pub pattern: String,
    pub file_format: BTreeMap<String, String>,
    /// TODO(xuanwo): parse into validation_mode directly.
    pub validation_mode: String,
    pub size_limit: usize,
}

/// CopyUnit is the unit that can be used in `COPY`.
#[derive(Debug, Clone, PartialEq)]
pub enum CopyUnit<'a> {
    /// Table can be used in `INTO` or `FROM`.
    ///
    /// While table used as `FROM`, it will be rewrite as `(SELECT * FROM table)`
    Table {
        catalog: Option<Identifier<'a>>,
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
    },
    /// StageLocation (a.k.a internal and external stage) can be used
    /// in `INTO` or `FROM`.
    ///
    /// For examples:
    ///
    /// - internal stage: `@internal_stage/path/to/dir/`
    /// - external stage: `@s3_external_stage/path/to/dir/`
    StageLocation {
        /// The name of the stage.
        name: String,
        path: String,
    },
    /// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
    ///
    /// For examples: `'s3://example/path/to/dir' CREDENTIALS = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
    ///
    /// TODO(xuanwo): Add endpoint_url support.
    /// TODO(xuanwo): We can check if we support this protocol during parsing.
    /// TODO(xuanwo): Maybe we can introduce more strict (friendly) report for credentials and encryption, like parsed into StorageConfig?
    UriLocation {
        protocol: String,
        name: String,
        path: String,
        credentials: BTreeMap<String, String>,
        encryption: BTreeMap<String, String>,
    },
    /// Query can only be used as `FROM`.
    ///
    /// For example:`(SELECT field_a,field_b FROM table)`
    Query(Box<Query<'a>>),
}

impl CopyUnit<'_> {
    pub fn target(&self) -> &'static str {
        match self {
            CopyUnit::Table { .. } => "Table",
            CopyUnit::StageLocation { .. } => "StageLocation",
            CopyUnit::UriLocation { .. } => "UriLocation",
            CopyUnit::Query(_) => "Query",
        }
    }
}

impl Display for CopyUnit<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyUnit::Table {
                catalog,
                database,
                table,
            } => {
                if let Some(catalog) = catalog {
                    write!(
                        f,
                        "{catalog}.{}.{table}",
                        database.as_ref().expect("database must be valid")
                    )
                } else if let Some(database) = database {
                    write!(f, "{database}.{table}")
                } else {
                    write!(f, "{table}")
                }
            }
            CopyUnit::StageLocation { name, path } => {
                write!(f, "@{name}{path}")
            }
            CopyUnit::UriLocation {
                protocol,
                name,
                path,
                credentials,
                encryption,
            } => {
                write!(f, "'{protocol}://{name}{path}'")?;
                if !credentials.is_empty() {
                    write!(f, " CREDENTIALS = ( ")?;
                    write_space_seperated_map(f, credentials)?;
                    write!(f, " )")?;
                }
                if !encryption.is_empty() {
                    write!(f, " ENCRYPTION = ( ")?;
                    write_space_seperated_map(f, encryption)?;
                    write!(f, " )")?;
                }
                Ok(())
            }
            CopyUnit::Query(query) => {
                write!(f, "({query})")
            }
        }
    }
}
