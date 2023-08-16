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

use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use parquet::file::metadata::ParquetMetaData;
use storages_common_pruner::RangePruner;
use storages_common_pruner::RangePrunerCreator;

use super::statistics::collect_row_group_stats;

/// A pruner to prune row groups and pages of a parquet files.
///
/// We can use this pruner to compute row groups and pages to skip.
pub struct ParquetPruner {
    schema: TableSchemaRef,
    row_group_pruner: Option<Arc<dyn RangePruner + Send + Sync>>,
}

impl ParquetPruner {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
        options: ParquetReadOptions,
    ) -> Result<Self> {
        let filter = push_down
            .as_ref()
            .and_then(|p| p.filter.as_ref().map(|f| f.as_expr(&BUILTIN_FUNCTIONS)));

        // TODO(parquet): Top-K in `push_down` can also help to prune.

        let row_group_pruner = if options.prune_row_groups() && filter.is_some() {
            let pruner = RangePrunerCreator::try_create(
                ctx.get_function_context()?,
                &schema,
                filter.as_ref(),
            )?;
            Some(pruner)
        } else {
            None
        };

        Ok(ParquetPruner {
            schema,
            row_group_pruner,
            // TODO(parquet): page pruner.
        })
    }

    /// Prune row groups of a parquet file.
    ///
    /// Return the selected row groups' indices in the meta.
    pub fn prune_row_groups(&self, meta: &ParquetMetaData) -> Result<Vec<usize>> {
        match &self.row_group_pruner {
            None => Ok((0..meta.num_row_groups()).collect()),
            Some(pruner) => {
                // Only if the file has row groups level statistics, we can use them to prune.
                if meta
                    .row_groups()
                    .iter()
                    .any(|rg| rg.columns().iter().any(|c| c.statistics().is_none()))
                {
                    return Ok((0..meta.num_row_groups()).collect());
                }
                let row_group_stats = collect_row_group_stats(&self.schema, meta.row_groups())?;
                let mut selection = Vec::with_capacity(meta.num_row_groups());

                for (i, row_group) in row_group_stats.iter().enumerate() {
                    if pruner.should_keep(row_group, None) {
                        selection.push(i);
                    }
                }

                Ok(selection)
            }
        }
    }
}
