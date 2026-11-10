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

use std::collections::BTreeMap;

use databend_storages_common_table_meta::table::stored_query_namespace;

/// How a table name is matched against the CTEs in scope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CteResolution {
    /// Only an unqualified name can refer to a CTE; `db.t` always names a table.
    #[default]
    UnqualifiedOnly,
    /// A qualified name also refers to a same-named CTE, whatever its qualifier.
    ///
    /// Only for stored definitions written before namespace options existed: their
    /// writer qualified CTE references with the definition's database.
    IgnoreQualifier,
}

impl CteResolution {
    /// Whether a table name, qualified or not, may refer to a CTE.
    pub fn may_reference_cte(self, qualified: bool) -> bool {
        !qualified || self == CteResolution::IgnoreQualifier
    }
}

/// Where the SQL being bound was written: an ordinary statement, or a stored
/// definition (view, dynamic table, ...) that must bind as it did when created,
/// independently of the session that reads it.
///
/// Constructors only produce the valid combinations: a stored namespace always
/// comes with [`CteResolution::UnqualifiedOnly`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DefinitionScope {
    namespace: Option<(String, String)>,
    cte_resolution: CteResolution,
}

impl DefinitionScope {
    /// Ordinary SQL: unqualified tables resolve in the session's catalog and database.
    pub fn session() -> Self {
        Self::default()
    }

    /// SQL written in `catalog.database`, e.g. a definition being created.
    pub fn in_namespace(catalog: String, database: String) -> Self {
        DefinitionScope {
            namespace: Some((catalog, database)),
            cte_resolution: CteResolution::UnqualifiedOnly,
        }
    }

    /// The scope of an existing stored definition, read from its table options.
    ///
    /// A definition without namespace options predates them. Its writer already
    /// qualified table references, so unqualified tables keep resolving in the
    /// session, and its qualified CTE references need
    /// [`CteResolution::IgnoreQualifier`].
    pub fn from_stored_options(options: &BTreeMap<String, String>) -> Self {
        match stored_query_namespace(options) {
            Some((catalog, database)) => Self::in_namespace(catalog, database),
            None => DefinitionScope {
                namespace: None,
                cte_resolution: CteResolution::IgnoreQualifier,
            },
        }
    }

    /// The catalog and database for unqualified tables, if not the session's.
    pub fn namespace(&self) -> Option<&(String, String)> {
        self.namespace.as_ref()
    }

    pub fn cte_resolution(&self) -> CteResolution {
        self.cte_resolution
    }

    /// Whether identical SQL text binds as it would in the session. Caches keyed by
    /// SQL text and session state are only valid in that case.
    pub fn is_session(&self) -> bool {
        *self == Self::session()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_definition_scope_from_stored_options() {
        let legacy = DefinitionScope::from_stored_options(&BTreeMap::new());
        assert_eq!(legacy.namespace(), None);
        assert_eq!(legacy.cte_resolution(), CteResolution::IgnoreQualifier);
        assert!(!legacy.is_session());
        assert!(legacy.cte_resolution().may_reference_cte(true));

        let options = [
            (
                databend_storages_common_table_meta::table::OPT_KEY_QUERY_CATALOG.to_string(),
                "default".to_string(),
            ),
            (
                databend_storages_common_table_meta::table::OPT_KEY_QUERY_DATABASE.to_string(),
                "db".to_string(),
            ),
        ]
        .into();
        let stored = DefinitionScope::from_stored_options(&options);
        assert_eq!(
            stored,
            DefinitionScope::in_namespace("default".to_string(), "db".to_string())
        );
        assert_eq!(stored.cte_resolution(), CteResolution::UnqualifiedOnly);
        assert!(!stored.is_session());
        assert!(!stored.cte_resolution().may_reference_cte(true));
        assert!(stored.cte_resolution().may_reference_cte(false));

        assert!(DefinitionScope::session().is_session());
    }
}
