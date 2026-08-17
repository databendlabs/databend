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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::VirtualPredicateRef;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_index::VirtualColumnStat;
use databend_storages_common_index::VirtualColumnStatsOfNames;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;
use log::warn;

pub struct RangeIndexInput<'a> {
    pub col_stats: &'a StatisticsOfColumns,
    pub spatial_stats: Option<&'a StatisticsOfSpatialColumns>,
    /// Range statistics of direct virtual columns referenced by filters or TopN.
    pub virtual_col_stats: Option<VirtualColumnStatsOfNames>,
}

impl<'a> RangeIndexInput<'a> {
    pub fn new(
        col_stats: &'a StatisticsOfColumns,
        spatial_stats: Option<&'a StatisticsOfSpatialColumns>,
    ) -> Self {
        Self {
            col_stats,
            spatial_stats,
            virtual_col_stats: None,
        }
    }

    pub fn from_columns(col_stats: &'a StatisticsOfColumns) -> Self {
        Self {
            col_stats,
            spatial_stats: None,
            virtual_col_stats: None,
        }
    }

    pub fn from_block_meta(
        block_meta: &'a BlockMeta,
        virtual_segment_schema: Option<&VirtualSegmentSchema>,
        virtual_predicate_refs: Option<&[VirtualPredicateRef]>,
    ) -> Self {
        Self {
            col_stats: &block_meta.col_stats,
            spatial_stats: block_meta.spatial_stats.as_ref(),
            virtual_col_stats: build_virtual_col_stats(
                block_meta,
                virtual_segment_schema,
                virtual_predicate_refs,
            ),
        }
    }

    pub fn virtual_column_statistics(&self) -> Option<StatisticsOfColumns> {
        self.virtual_col_stats.as_ref().map(|stats| {
            stats
                .values()
                .map(|stat| (stat.query_column_id, stat.to_column_statistics()))
                .collect()
        })
    }
}

fn build_virtual_col_stats(
    block_meta: &BlockMeta,
    virtual_schema: Option<&VirtualSegmentSchema>,
    virtual_refs: Option<&[VirtualPredicateRef]>,
) -> Option<VirtualColumnStatsOfNames> {
    let virtual_refs = virtual_refs.filter(|refs| !refs.is_empty())?;
    let virtual_meta = block_meta.virtual_block_meta.as_ref()?;
    let schema = virtual_schema?;

    let mut stats = HashMap::with_capacity(virtual_refs.len());
    for virtual_ref in virtual_refs {
        let Some(path) =
            schema.find_path_ref(virtual_ref.source_column_id, &virtual_ref.encoded_path)
        else {
            continue;
        };
        let Some(column) = virtual_meta.virtual_column_metas.get(&path.column_id) else {
            continue;
        };
        let Some(column_stat) = column.column_stat.as_ref() else {
            continue;
        };
        let Some(scalar_data_type) = column_stat
            .min
            .as_ref()
            .infer_common_type(&column_stat.max.as_ref())
        else {
            continue;
        };
        let physical_data_type = column.data_type();
        let physical_expression_type = DataType::from(&physical_data_type);
        if matches!(
            physical_expression_type.remove_nullable(),
            DataType::Variant
        ) || physical_expression_type.remove_nullable() != scalar_data_type.remove_nullable()
        {
            continue;
        }
        stats.insert(virtual_ref.name.clone(), VirtualColumnStat {
            query_column_id: virtual_ref.query_column_id,
            min: column_stat.min.clone(),
            max: column_stat.max.clone(),
            null_count: column_stat.null_count,
            data_type: physical_data_type,
        });
    }
    (!stats.is_empty()).then_some(stats)
}

pub trait RangePruner {
    // returns true, if target should NOT be pruned (false positive allowed)
    fn should_keep(
        &self,
        input: &RangeIndexInput,
        metas: Option<&HashMap<ColumnId, ColumnMeta>>,
    ) -> bool;

    fn should_keep_with_partition_columns(
        &self,
        _stats: &StatisticsOfColumns,
        _partition_columns: Option<&HashMap<String, Scalar>>,
    ) -> bool {
        true
    }
}

struct KeepTrue;

impl RangePruner for KeepTrue {
    fn should_keep(
        &self,
        _input: &RangeIndexInput,
        _metas: Option<&HashMap<ColumnId, ColumnMeta>>,
    ) -> bool {
        true
    }
}

struct KeepFalse;

impl RangePruner for KeepFalse {
    fn should_keep(
        &self,
        _input: &RangeIndexInput,
        _metas: Option<&HashMap<ColumnId, ColumnMeta>>,
    ) -> bool {
        false
    }
}

impl RangePruner for RangeIndex {
    fn should_keep(
        &self,
        input: &RangeIndexInput,
        metas: Option<&HashMap<ColumnId, ColumnMeta>>,
    ) -> bool {
        let apply = self.apply(
            input.col_stats,
            input.spatial_stats,
            input.virtual_col_stats.as_ref(),
            |k| {
                if let Some(metas) = metas {
                    metas.get(k).is_none()
                } else {
                    false
                }
            },
        );

        match apply {
            Ok(r) => r,
            Err(e) => {
                // swallow exceptions intentionally, corrupted index should not prevent execution
                warn!("failed to range filter, returning true. {}", e);
                true
            }
        }
    }
    fn should_keep_with_partition_columns(
        &self,
        stats: &StatisticsOfColumns,
        partition_columns: Option<&HashMap<String, Scalar>>,
    ) -> bool {
        match partition_columns {
            None => self.should_keep(&RangeIndexInput::from_columns(stats), None),
            Some(partition_columns) => {
                match self.apply_with_partition_columns(stats, partition_columns) {
                    Ok(r) => r,
                    Err(e) => {
                        // swallow exceptions intentionally, corrupted index should not prevent execution
                        warn!("failed to range filter, returning true. {}", e);
                        true
                    }
                }
            }
        }
    }
}

pub struct RangePrunerCreator;

impl RangePrunerCreator {
    /// Create a new [`RangePruner`] from expression and schema.
    ///
    /// Note: the schema should be the schema of the table, not the schema of the input.
    pub fn try_create<'a>(
        func_ctx: FunctionContext,
        schema: &'a TableSchemaRef,
        filter_expr: Option<&'a Expr<String>>,
    ) -> Result<Arc<dyn RangePruner + Send + Sync>> {
        Self::try_create_with_default_stats(
            func_ctx,
            schema,
            filter_expr,
            StatisticsOfColumns::default(),
        )
    }

    pub fn try_create_with_default_stats<'a>(
        func_ctx: FunctionContext,
        schema: &'a TableSchemaRef,
        filter_expr: Option<&'a Expr<String>>,
        default_stats: StatisticsOfColumns,
    ) -> Result<Arc<dyn RangePruner + Send + Sync>> {
        let Some(exprs) = filter_expr else {
            return Ok(Arc::new(KeepTrue));
        };

        let range_filter = RangeIndex::try_create(func_ctx, exprs, schema.clone(), default_stats)?;
        if let Ok(false) = range_filter.try_apply_const() {
            return Ok(Arc::new(KeepFalse));
        }

        Ok(Arc::new(range_filter))
    }
}
