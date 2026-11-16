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

use std::collections::HashSet;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// The table of a request. Names are exact (case-sensitive), as stored in the catalog,
/// like the names of the catalog HTTP endpoints; an omitted catalog or database is the
/// session's current one.
///
/// Each request type holds these fields itself, so the JSON of every operation is flat
/// and rejects unknown fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableRef<'a> {
    pub catalog: Option<&'a str>,
    pub database: Option<&'a str>,
    pub table: &'a str,
}

/// Rejects the output columns of an `op` request listing no column or a column twice.
pub fn validate_output(op: &str, output: &[String]) -> Result<()> {
    if output.is_empty() {
        return Err(ErrorCode::BadArguments(format!(
            "{op} output must list a column"
        )));
    }
    let mut seen = HashSet::with_capacity(output.len());
    if let Some(name) = output.iter().find(|name| !seen.insert(name.as_str())) {
        return Err(ErrorCode::BadArguments(format!(
            "{op} output lists column `{name}` twice"
        )));
    }
    Ok(())
}
