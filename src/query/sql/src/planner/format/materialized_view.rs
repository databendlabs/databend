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

use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::TableMeta;

/// Format the canonical CREATE statement shared by SHOW CREATE and system tables.
pub fn format_materialized_view_create_sql(
    database: &str,
    name: &str,
    definition: &MVDefinition,
    table_meta: &TableMeta,
) -> String {
    let mut create_sql = format!(
        "CREATE MATERIALIZED VIEW {}.{}",
        QuotedIdent(database, '`'),
        QuotedIdent(name, '`')
    );

    let columns = definition
        .logical_schema
        .fields()
        .iter()
        .map(|field| QuotedIdent(field.name(), '`').to_string())
        .collect::<Vec<_>>()
        .join(", ");
    create_sql.push_str(&format!(" ({columns})"));

    if let Some(cluster_key) = table_meta.cluster_key_str() {
        create_sql.push_str(&format!(" CLUSTER BY {cluster_key}"));
    }

    create_sql.push_str(&format!(" AS {}", definition.original_query));
    create_sql
}
