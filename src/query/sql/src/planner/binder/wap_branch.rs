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

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::database::is_system_database;
use databend_common_exception::Result;

use crate::binder::Binder;

impl Binder {
    /// Resolve the write-audit-publish branch of a DML target table.
    ///
    /// Like Iceberg's `spark.wap.branch`, `wap_branch` only supplies an
    /// implicit branch for writes. Reads and non-DML statements use the base
    /// table unless they contain an explicit `table/branch` reference.
    pub(crate) fn resolve_wap_target_branch(
        &self,
        catalog: &str,
        database: &str,
        table_name: &str,
        explicit_branch: Option<String>,
        suppress_wap_branch: bool,
    ) -> Result<Option<String>> {
        if explicit_branch.is_some() || suppress_wap_branch {
            return Ok(explicit_branch);
        }

        if catalog != CATALOG_DEFAULT
            || is_system_database(database)
            || self.ctx.is_temp_table(catalog, database, table_name)
        {
            return Ok(None);
        }

        // DML targets never silently fall back to the base table. The target
        // lookup reports an error if the configured branch does not exist or
        // the table engine does not support branches.
        self.ctx.get_settings().get_wap_branch()
    }
}
