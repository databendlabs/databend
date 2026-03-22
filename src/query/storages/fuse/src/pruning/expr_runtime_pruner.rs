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
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::statistics_to_domain;
use databend_storages_common_io::ReadSettings;
use log::info;
use log::warn;
use opendal::Operator;

use crate::FuseBlockPartInfo;
use crate::pruning::bloom_pruner::should_prune_runtime_inlist_by_bloom_index;

/// Runtime pruner that uses expressions to prune partitions.
pub struct ExprRuntimePruner {
    func_ctx: FunctionContext,
    table_schema: TableSchemaRef,
    dal: Operator,
    settings: ReadSettings,
    inlist_bloom_prune_threshold: usize,
    exprs: Vec<RuntimeFilterExpr>,
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterExprKind {
    Inlist,
    MinMax,
}

#[derive(Clone)]
pub struct RuntimeFilterExpr {
    pub filter_id: usize,
    pub kind: RuntimeFilterExprKind,
    pub inlist_value_count: usize,
    pub expr: Expr<String>,
    pub stats: Arc<RuntimeFilterStats>,
}

impl ExprRuntimePruner {
    /// Create a new expression runtime pruner.
    pub fn new(
        func_ctx: FunctionContext,
        table_schema: TableSchemaRef,
        dal: Operator,
        settings: ReadSettings,
        inlist_bloom_prune_threshold: usize,
        exprs: Vec<RuntimeFilterExpr>,
    ) -> Self {
        Self {
            func_ctx,
            table_schema,
            dal,
            settings,
            inlist_bloom_prune_threshold,
            exprs,
        }
    }

    /// Prune a partition based on expressions.
    /// Returns true if the partition should be pruned.
    pub async fn prune(&self, part: &PartInfoPtr) -> Result<bool> {
        if self.exprs.is_empty() {
            return Ok(false);
        }

        let part = FuseBlockPartInfo::from_part(part)?;
        let mut partition_pruned = false;
        for entry in self.exprs.iter() {
            let start = Instant::now();
            let filter = &entry.expr;
            let mut should_prune = self.prune_by_statistics(filter, part);

            if !should_prune
                && matches!(entry.kind, RuntimeFilterExprKind::Inlist)
                && entry.inlist_value_count > 0
                && entry.inlist_value_count <= self.inlist_bloom_prune_threshold
            {
                should_prune = match self.prune_inlist_by_bloom_index(filter, part).await {
                    Ok(pruned) => pruned,
                    Err(err) => {
                        warn!(
                            "failed to prune runtime inlist filter #{} by bloom index, returning keep block. {}",
                            entry.filter_id, err
                        );
                        false
                    }
                };
            }

            let elapsed = start.elapsed();
            entry.stats.record_inlist_min_max(
                elapsed.as_nanos() as u64,
                if should_prune {
                    part.nums_rows as u64
                } else {
                    0
                },
                if should_prune { 1 } else { 0 },
            );

            if should_prune {
                partition_pruned = true;
                break;
            }
        }

        if partition_pruned {
            Profile::record_usize_profile(ProfileStatisticsName::RuntimeFilterPruneParts, 1);
        }

        Ok(partition_pruned)
    }

    fn prune_by_statistics(&self, filter: &Expr<String>, part: &FuseBlockPartInfo) -> bool {
        if matches!(
            filter,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        ) {
            return true;
        }

        let column_refs = filter.column_refs();
        debug_assert!(column_refs.len() == 1);
        let ty = column_refs.values().last().unwrap();
        let name = column_refs.keys().last().unwrap();

        if let Some(stats) = &part.columns_stat {
            let column_ids = self.table_schema.leaf_columns_of(name);
            if column_ids.len() == 1 {
                if let Some(stat) = stats.get(&column_ids[0]) {
                    let stats = vec![stat];
                    let domain = statistics_to_domain(stats, ty);

                    let mut input_domains = HashMap::new();
                    input_domains.insert(name.to_string(), domain.clone());

                    let (new_expr, _) = ConstantFolder::fold_with_domain(
                        filter,
                        &input_domains,
                        &self.func_ctx,
                        &BUILTIN_FUNCTIONS,
                    );
                    return matches!(
                        new_expr,
                        Expr::Constant(Constant {
                            scalar: Scalar::Boolean(false),
                            ..
                        })
                    );
                }
            }
        } else {
            info!(
                "Can't prune the partition by runtime filter, because there is no statistics for the partition"
            );
        }

        false
    }

    async fn prune_inlist_by_bloom_index(
        &self,
        filter: &Expr<String>,
        part: &FuseBlockPartInfo,
    ) -> Result<bool> {
        should_prune_runtime_inlist_by_bloom_index(
            &self.func_ctx,
            &self.dal,
            &self.settings,
            &self.table_schema,
            filter,
            part,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_base::base::GlobalInstance;
    use databend_common_base::base::tokio;
    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_config::CacheConfig;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Expr;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::UInt64Type;
    use databend_storages_common_blocks::blocks_to_parquet;
    use databend_storages_common_cache::CacheManager;
    use databend_storages_common_index::BloomIndexBuilder;
    use databend_storages_common_index::filters::BlockFilter;
    use databend_storages_common_io::ReadSettings;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::Compression;
    use databend_storages_common_table_meta::meta::Versioned;
    use databend_storages_common_table_meta::table::TableCompression;

    use super::*;

    fn test_read_settings() -> ReadSettings {
        ReadSettings {
            max_gap_size: 0,
            max_range_size: 0,
            parquet_fast_read_bytes: u64::MAX,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_runtime_inlist_prunes_by_bloom_index() -> Result<()> {
        init_test_globals()?;
        let schema = test_schema();
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![10, 20, 30, 40])]);
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;
        let (index_location, index_size) = write_bloom_index(&operator, &schema, &block).await?;
        let part = make_part(&schema, index_location, index_size, 10, 40);
        let expr = inlist_expr("y", &[11, 21, 31])?;
        let stats = Arc::new(RuntimeFilterStats::default());
        let pruner = ExprRuntimePruner::new(
            FunctionContext::default(),
            schema,
            operator,
            test_read_settings(),
            3,
            vec![RuntimeFilterExpr {
                filter_id: 0,
                kind: RuntimeFilterExprKind::Inlist,
                inlist_value_count: 3,
                expr,
                stats: stats.clone(),
            }],
        );

        assert!(pruner.prune(&part).await?);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.min_max_rows_filtered, 4);
        assert_eq!(snapshot.min_max_partitions_pruned, 1);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_runtime_inlist_keeps_block_when_any_value_may_exist() -> Result<()> {
        init_test_globals()?;
        let schema = test_schema();
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![10, 20, 30, 40])]);
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;
        let (index_location, index_size) = write_bloom_index(&operator, &schema, &block).await?;
        let part = make_part(&schema, index_location, index_size, 10, 40);
        let pruner = ExprRuntimePruner::new(
            FunctionContext::default(),
            schema,
            operator,
            test_read_settings(),
            3,
            vec![RuntimeFilterExpr {
                filter_id: 0,
                kind: RuntimeFilterExprKind::Inlist,
                inlist_value_count: 3,
                expr: inlist_expr("y", &[11, 20, 31])?,
                stats: Arc::new(RuntimeFilterStats::default()),
            }],
        );

        assert!(!pruner.prune(&part).await?);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_runtime_inlist_keeps_block_without_bloom_index() -> Result<()> {
        init_test_globals()?;
        let schema = test_schema();
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;
        let part = make_part(&schema, None, 0, 10, 40);
        let pruner = ExprRuntimePruner::new(
            FunctionContext::default(),
            schema,
            operator,
            test_read_settings(),
            3,
            vec![RuntimeFilterExpr {
                filter_id: 0,
                kind: RuntimeFilterExprKind::Inlist,
                inlist_value_count: 3,
                expr: inlist_expr("y", &[11, 21, 31])?,
                stats: Arc::new(RuntimeFilterStats::default()),
            }],
        );

        assert!(!pruner.prune(&part).await?);
        Ok(())
    }

    fn test_schema() -> TableSchemaRef {
        Arc::new(TableSchema::new(vec![TableField::new(
            "y",
            TableDataType::Number(NumberDataType::Int32),
        )]))
    }

    fn init_test_globals() -> Result<()> {
        static INIT: OnceLock<Result<()>> = OnceLock::new();
        INIT.get_or_init(|| {
            GlobalInstance::init_production();
            GlobalIORuntime::init(1)?;
            CacheManager::init(&CacheConfig::default(), &(1024 * 1024), "test", false)
        })
        .clone()
    }

    fn make_part(
        schema: &TableSchemaRef,
        bloom_filter_index_location: Option<(String, u64)>,
        bloom_filter_index_size: u64,
        min: i32,
        max: i32,
    ) -> databend_common_catalog::plan::PartInfoPtr {
        let column_id = schema.column_id_of("y").unwrap();
        let mut column_stats = HashMap::new();
        column_stats.insert(
            column_id,
            ColumnStatistics::new(
                Scalar::Number(NumberScalar::Int32(min)),
                Scalar::Number(NumberScalar::Int32(max)),
                0,
                0,
                None,
            ),
        );

        FuseBlockPartInfo::create(
            "memory:///block".to_string(),
            bloom_filter_index_location,
            bloom_filter_index_size,
            None,
            0,
            4,
            HashMap::new(),
            Some(column_stats),
            None,
            Compression::Lz4Raw,
            None,
            None,
            None,
        )
    }

    fn inlist_expr(column_name: &str, values: &[i32]) -> Result<Expr<String>> {
        let column = Expr::ColumnRef(ColumnRef {
            span: None,
            id: column_name.to_string(),
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: column_name.to_string(),
        });

        let eq_exprs = values
            .iter()
            .map(|value| {
                let constant = Expr::Constant(Constant {
                    span: None,
                    scalar: Scalar::Number(NumberScalar::Int32(*value)),
                    data_type: DataType::Number(NumberDataType::Int32),
                });
                check_function(
                    None,
                    "eq",
                    &[],
                    &[column.clone(), constant],
                    &BUILTIN_FUNCTIONS,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        if eq_exprs.len() == 1 {
            return Ok(eq_exprs[0].clone());
        }

        check_function(None, "or_filters", &[], &eq_exprs, &BUILTIN_FUNCTIONS)
    }

    async fn write_bloom_index(
        operator: &opendal::Operator,
        schema: &TableSchemaRef,
        block: &DataBlock,
    ) -> Result<(Option<(String, u64)>, u64)> {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let (_, field) = schema.column_with_name("y").unwrap();
        let bloom_columns_map = BTreeMap::from([(0usize, field.clone())]);
        let mut builder =
            BloomIndexBuilder::create(FunctionContext::default(), bloom_columns_map, &[])?;
        builder.add_block(block)?;
        let bloom_index = builder.finalize()?.unwrap();

        let index_block = bloom_index.serialize_to_data_block()?;
        let location = (
            format!("block_bloom_{}", NEXT_ID.fetch_add(1, Ordering::Relaxed)),
            BlockFilter::VERSION,
        );
        let mut data = Vec::new();
        let _ = blocks_to_parquet(
            &bloom_index.filter_schema,
            vec![index_block],
            &mut data,
            TableCompression::None,
            false,
            None,
        )?;
        let size = data.len() as u64;
        operator.write(&location.0, data).await?;
        Ok((Some(location), size))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_runtime_inlist_keeps_block_when_threshold_too_small() -> Result<()> {
        init_test_globals()?;
        let schema = test_schema();
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![10, 20, 30, 40])]);
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;
        let (index_location, index_size) = write_bloom_index(&operator, &schema, &block).await?;
        let part = make_part(&schema, index_location, index_size, 10, 40);
        let pruner = ExprRuntimePruner::new(
            FunctionContext::default(),
            schema,
            operator,
            test_read_settings(),
            2,
            vec![RuntimeFilterExpr {
                filter_id: 0,
                kind: RuntimeFilterExprKind::Inlist,
                inlist_value_count: 3,
                expr: inlist_expr("y", &[11, 21, 31])?,
                stats: Arc::new(RuntimeFilterStats::default()),
            }],
        );

        assert!(!pruner.prune(&part).await?);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_runtime_inlist_prunes_u64_block_with_wide_min_max() -> Result<()> {
        init_test_globals()?;
        let schema = test_schema_u64();
        let values = (0u64..1000)
            .map(|value| value * 100 + 42)
            .collect::<Vec<_>>();
        let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(values)]);
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;
        let (index_location, index_size) = write_bloom_index(&operator, &schema, &block).await?;
        let part = make_part_u64(&schema, index_location, index_size, 42, 99942);
        let pruner = ExprRuntimePruner::new(
            FunctionContext::default(),
            schema,
            operator,
            test_read_settings(),
            10,
            vec![RuntimeFilterExpr {
                filter_id: 0,
                kind: RuntimeFilterExprKind::Inlist,
                inlist_value_count: 10,
                expr: inlist_expr_u64("y", &(50000u64..50010).collect::<Vec<_>>())?,
                stats: Arc::new(RuntimeFilterStats::default()),
            }],
        );

        assert!(pruner.prune(&part).await?);
        Ok(())
    }

    fn test_schema_u64() -> TableSchemaRef {
        Arc::new(TableSchema::new(vec![TableField::new(
            "y",
            TableDataType::Number(NumberDataType::UInt64),
        )]))
    }

    fn make_part_u64(
        schema: &TableSchemaRef,
        bloom_filter_index_location: Option<(String, u64)>,
        bloom_filter_index_size: u64,
        min: u64,
        max: u64,
    ) -> databend_common_catalog::plan::PartInfoPtr {
        let column_id = schema.column_id_of("y").unwrap();
        let mut column_stats = HashMap::new();
        column_stats.insert(
            column_id,
            ColumnStatistics::new(
                Scalar::Number(NumberScalar::UInt64(min)),
                Scalar::Number(NumberScalar::UInt64(max)),
                0,
                0,
                None,
            ),
        );

        FuseBlockPartInfo::create(
            "memory:///block".to_string(),
            bloom_filter_index_location,
            bloom_filter_index_size,
            None,
            0,
            1000,
            HashMap::new(),
            Some(column_stats),
            None,
            Compression::Lz4Raw,
            None,
            None,
            None,
        )
    }

    fn inlist_expr_u64(column_name: &str, values: &[u64]) -> Result<Expr<String>> {
        let column = Expr::ColumnRef(ColumnRef {
            span: None,
            id: column_name.to_string(),
            data_type: DataType::Number(NumberDataType::UInt64),
            display_name: column_name.to_string(),
        });

        let eq_exprs = values
            .iter()
            .map(|value| {
                let constant = Expr::Constant(Constant {
                    span: None,
                    scalar: Scalar::Number(NumberScalar::UInt64(*value)),
                    data_type: DataType::Number(NumberDataType::UInt64),
                });
                check_function(
                    None,
                    "eq",
                    &[],
                    &[column.clone(), constant],
                    &BUILTIN_FUNCTIONS,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        if eq_exprs.len() == 1 {
            return Ok(eq_exprs[0].clone());
        }

        check_function(None, "or_filters", &[], &eq_exprs, &BUILTIN_FUNCTIONS)
    }
}
