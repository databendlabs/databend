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

use sqllogictest::Runner;

use crate::report::NonDefaultSetting;
use crate::util::ColumnType;

const LAST_QUERY_ID_COLUMN_TYPES: &str = "T";
const LAST_QUERY_ID_SQL: &str = "SELECT LAST_QUERY_ID()";
const NON_DEFAULT_SETTINGS_COLUMN_TYPES: &str = "TTTT";
const NON_DEFAULT_SETTINGS_SQL: &str = "SELECT name, value, default, level \
     FROM system.settings \
     WHERE value <> default \
     ORDER BY name";

pub(crate) struct FailureDiagnostics {
    pub(crate) query_id: Option<String>,
    pub(crate) non_default_settings: Vec<NonDefaultSetting>,
}

pub(crate) trait DiagnosticsQueryExecutor {
    async fn query_rows(&mut self, column_types: &str, sql: &str) -> Option<Vec<Vec<String>>>;
}

impl<D, M> DiagnosticsQueryExecutor for Runner<D, M>
where
    D: sqllogictest::AsyncDB<ColumnType = ColumnType>,
    M: sqllogictest::MakeConnection<Conn = D>,
{
    async fn query_rows(&mut self, column_types: &str, sql: &str) -> Option<Vec<Vec<String>>> {
        let script = format!("query {column_types}\n{sql}\n----\n");
        let records = sqllogictest::parse::<ColumnType>(&script).ok()?;
        let record = records.into_iter().next()?;
        if let sqllogictest::RecordOutput::Query { rows, .. } = self.apply_record(record).await {
            Some(rows)
        } else {
            None
        }
    }
}

pub(crate) async fn capture_failure_diagnostics(
    executor: &mut impl DiagnosticsQueryExecutor,
) -> FailureDiagnostics {
    let query_id = executor
        .query_rows(LAST_QUERY_ID_COLUMN_TYPES, LAST_QUERY_ID_SQL)
        .await
        .and_then(extract_last_query_id);
    let non_default_settings = executor
        .query_rows(NON_DEFAULT_SETTINGS_COLUMN_TYPES, NON_DEFAULT_SETTINGS_SQL)
        .await
        .map(extract_non_default_settings)
        .unwrap_or_default();

    FailureDiagnostics {
        query_id,
        non_default_settings,
    }
}

fn extract_last_query_id(rows: Vec<Vec<String>>) -> Option<String> {
    rows.into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
}

fn extract_non_default_settings(rows: Vec<Vec<String>>) -> Vec<NonDefaultSetting> {
    rows.into_iter()
        .filter_map(|row| match row.as_slice() {
            [name, value, default_value, level] => Some(NonDefaultSetting::new(
                name.clone(),
                value.clone(),
                default_value.clone(),
                level.clone(),
            )),
            _ => None,
        })
        .collect()
}
