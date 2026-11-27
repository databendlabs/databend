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

use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::TypeChecker;
use databend_common_sql::bind_table;
use databend_common_sql::materialize_ttl_predicate;
use databend_common_storages_fuse::operations::TtlDrop;
use databend_storages_common_table_meta::table::is_fuse_engine;
use log::warn;

use crate::physical_plans::create_push_down_filters;
use crate::sessions::TableContext;

/// The table TTL, bound once per maintenance statement for COMPACT and RECLUSTER
/// to drop the blocks whose statistics prove every row expired. The cutoff is a
/// constant of the predicate, so all rounds and nodes of the statement agree.
#[derive(Clone)]
pub(crate) struct MaintenanceTtl {
    /// The TTL definition the commit revalidates on retry.
    pub definition: String,
    /// The MATERIALIZE TTL predicate and its inverse.
    pub filters: Filters,
}

impl MaintenanceTtl {
    /// Bind the TTL of `tbl` for maintenance, or `None` where maintenance does not
    /// apply it: the tables MATERIALIZE TTL supports, except column-oriented
    /// segments, outside explicit transactions, whose deferred commit cannot
    /// revalidate the TTL. Never fails maintenance: a binding error only skips TTL.
    pub fn bind(ctx: &Arc<dyn TableContext>, tbl: &Arc<dyn Table>) -> Option<Self> {
        match Self::try_bind(ctx, tbl) {
            Ok(ttl) => ttl,
            Err(e) => {
                warn!(
                    "skip TTL in maintenance of table {}: {e}",
                    tbl.get_table_info().desc
                );
                None
            }
        }
    }

    fn try_bind(ctx: &Arc<dyn TableContext>, tbl: &Arc<dyn Table>) -> Result<Option<Self>> {
        let table_info = tbl.get_table_info();
        let Some(definition) = table_info.meta.ttl.clone() else {
            return Ok(None);
        };
        if ctx.txn_mgr().lock().is_active()
            || !is_fuse_engine(tbl.engine())
            || tbl.is_temp()
            || table_info.meta.options.contains_key("TRANSIENT")
            || tbl.is_column_oriented()
            || tbl.check_mutable_for_maintenance().is_err()
        {
            return Ok(None);
        }

        let predicate = materialize_ttl_predicate(ctx.clone(), &tbl.schema(), &definition)?;
        let settings = ctx.get_settings();
        let (mut bind_context, metadata) = bind_table(tbl.clone())?;
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let mut type_checker = TypeChecker::try_create(
            &mut bind_context,
            ctx.clone(),
            &name_resolution_ctx,
            metadata,
            &[],
            true,
        )?;
        let (scalar, _) = *type_checker.resolve(&predicate)?;
        let filters = create_push_down_filters(&ctx.get_function_context()?, &[scalar])?;
        Ok(Some(Self {
            definition,
            filters,
        }))
    }

    /// Whether `tbl` still has the TTL this was bound from.
    pub fn applies_to(&self, tbl: &dyn Table) -> bool {
        tbl.get_table_info().meta.ttl.as_deref() == Some(self.definition.as_str())
    }

    pub fn build(&self, ctx: &Arc<dyn TableContext>, tbl: &dyn Table) -> Result<Arc<TtlDrop>> {
        Ok(Arc::new(TtlDrop::try_create(
            ctx.get_function_context()?,
            &self.filters,
            tbl.schema(),
        )?))
    }
}
