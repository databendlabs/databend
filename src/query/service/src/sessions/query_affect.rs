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

use serde::Deserialize;
use serde::Serialize;

/// We take a client side approach to handle session state during the query execution.
/// Each query is actually stateless. The client should apply the query affects itself,
/// and send the affects back to the server on reconnect.
///
/// For example, a user might switch the current database during a SQL script, like:
///
/// ```sql
/// SELECT * FROM mytable1;
/// USE mydb2;  -- switch to another database
/// SELECT * FROM mytable2;
/// ```
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
pub enum QueryAffect {
    #[allow(unused)]
    Create {
        kind: String,
        name: String,
        success: bool,
    },
    UseDB {
        name: String,
    },
    ChangeSettings {
        // The new settings' keys & values after each `SET key=value, key=value` statement.
        // The client could save this query affect and restore the settings when reconnect.
        keys: Vec<String>,
        values: Vec<String>,
        // Reset is set as true when `UNSET <settings>` is executed. The client could
        // forget about this settings on `is_reset` is set as true.
        is_unset: bool,
        is_globals: Vec<bool>,
    },
}
