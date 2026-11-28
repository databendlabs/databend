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

//! Granule-level indexes narrow a block's sparse-index survivor ranges before data-page reads.

mod bloom;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use opendal::Buffer;
use opendal::Operator;

use crate::io::GranulePruningReadContext;
use crate::statistics::ClusterStatsGenerator;

/// Materialize the scalar cluster-key columns used by the sparse granule-min index without
/// changing the physical block written to storage. Expression keys may have been removed after
/// cluster statistics were generated, or may never have been present on mutation rewrite paths.
pub fn materialize_cluster_key_columns(
    block: &DataBlock,
    generator: &ClusterStatsGenerator,
    offsets: Option<Vec<usize>>,
) -> Result<Option<Vec<Column>>> {
    let Some(offsets) = offsets else {
        return Ok(None);
    };

    let evaluated;
    let block = if offsets.iter().all(|offset| *offset < block.num_columns()) {
        block
    } else {
        evaluated = generator
            .eval_operators
            .iter()
            .try_fold(block.clone(), |input, operator| {
                operator.execute(&generator.func_ctx, input)
            })?;
        &evaluated
    };

    offsets
        .into_iter()
        .map(|offset| {
            if offset >= block.num_columns() {
                return Err(ErrorCode::Internal(format!(
                    "cluster-key column offset {offset} is out of bounds for granule index block with {} columns",
                    block.num_columns()
                )));
            }
            Ok(block.get_by_offset(offset).to_column())
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

#[derive(Debug, Clone)]
pub struct GranuleMark {
    pub field: TableField,
    pub values: Column,
}

impl GranuleMark {
    pub fn create(name: &str, values: Vec<u64>) -> Self {
        Self {
            field: TableField::new(name, TableDataType::Number(NumberDataType::UInt64)),
            values: UInt64Type::from_data(values),
        }
    }
}

#[derive(Debug)]
pub struct PendingGranuleIndexPayload {
    pub location: Location,
    pub data: Buffer,
}

#[derive(Default)]
pub struct PendingGranuleIndexOutput {
    pub marks: Vec<GranuleMark>,
    pub pending_payloads: Vec<PendingGranuleIndexPayload>,
}

impl PendingGranuleIndexOutput {
    pub fn merge(&mut self, other: PendingGranuleIndexOutput) -> Result<()> {
        merge_marks(&mut self.marks, other.marks)?;
        self.pending_payloads.extend(other.pending_payloads);
        Ok(())
    }
}

fn merge_marks(target: &mut Vec<GranuleMark>, marks: Vec<GranuleMark>) -> Result<()> {
    for mark in marks {
        if target
            .iter()
            .any(|existing| existing.field.name() == mark.field.name())
        {
            return Err(ErrorCode::Internal(format!(
                "duplicate granule mark {}",
                mark.field.name()
            )));
        }
        target.push(mark);
    }
    Ok(())
}

/// Granule-index writer driven by complete `DataBlock` inputs. The caller seals granules at
/// exactly the same boundaries as the main Parquet writer.
pub trait GranuleIndexWriter: Send {
    fn write(&mut self, block: &DataBlock, range: Range<usize>) -> Result<()>;

    fn finish_granule(&mut self) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<PendingGranuleIndexOutput>;
}

pub(super) struct NoopGranuleIndexWriter;

impl GranuleIndexWriter for NoopGranuleIndexWriter {
    fn write(&mut self, _block: &DataBlock, _range: Range<usize>) -> Result<()> {
        Ok(())
    }

    fn finish_granule(&mut self) -> Result<()> {
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<PendingGranuleIndexOutput> {
        Ok(PendingGranuleIndexOutput::default())
    }
}

pub const GRANULE_BLOOM_INDEX_NAME: &str = "bloom";

pub trait GranuleIndexPruner: Send + Sync {
    fn name(&self) -> &'static str;

    fn required_marks(&self) -> Vec<String>;

    fn prune_granules(
        &self,
        block_meta: &BlockMeta,
        input: &[Range<usize>],
        read_ctx: &GranulePruningReadContext,
    ) -> Result<Vec<Range<usize>>>;
}

/// Factory shared by granule-index write and read paths.
pub trait GranuleIndexSpec: Send + Sync {
    /// Return external payload object keys derived from a block location.
    ///
    /// Vacuum collects keys from all active specs and deletes them in batches. Implementations
    /// must not perform I/O here; payloads from dropped specs are handled by the orphan sweep.
    fn payload_locations(&self, block_location: &str) -> Vec<String>;

    /// Default writers bind stable IDs and return compressed pending payloads.
    fn new_writer(
        &self,
        func_ctx: FunctionContext,
        physical_schema: &TableSchema,
        block_location: &str,
    ) -> Result<Box<dyn GranuleIndexWriter>>;

    fn new_pruner(
        &self,
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        dal: Operator,
        settings: ReadSettings,
    ) -> Result<Option<Arc<dyn GranuleIndexPruner>>>;
}

pub fn collect_granule_index_payload_locations(
    specs: &[Arc<dyn GranuleIndexSpec>],
    block_locations: &[String],
) -> Vec<String> {
    block_locations
        .iter()
        .flat_map(|block_location| {
            specs
                .iter()
                .flat_map(|spec| spec.payload_locations(block_location))
        })
        .collect()
}

pub fn build_granule_index_specs(
    indexes: &BTreeMap<String, TableIndex>,
    schema: &TableSchema,
) -> Result<Vec<Arc<dyn GranuleIndexSpec>>> {
    let mut specs: Vec<Arc<dyn GranuleIndexSpec>> = Vec::new();
    for (name, index) in indexes.iter() {
        if index.index_type == TableIndexType::Bloom
            && let Some(spec) = bloom::BloomGranuleIndexSpec::try_create(name, index, schema)?
        {
            specs.push(Arc::new(spec));
        }
    }
    Ok(specs)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockThresholds;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::DataField;
    use databend_common_expression::Expr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberType;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use databend_common_sql::evaluator::BlockOperator;

    use super::*;
    use crate::statistics::ClusterStatsKey;
    use crate::statistics::ClusterStatsLayout;

    fn int64(value: i64) -> Expr {
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Number(value.into()),
            data_type: DataType::Number(NumberDataType::Int64),
        })
    }

    #[test]
    fn test_materialize_expression_cluster_key_columns() {
        let column = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Number(NumberDataType::Int64),
            display_name: "a".to_string(),
        });
        let key =
            check_function(None, "modulo", &[], &[column, int64(3)], &BUILTIN_FUNCTIONS).unwrap();
        let generator = ClusterStatsGenerator::new(
            0,
            vec![ClusterStatsKey {
                offset: 1,
                source_column_id: None,
            }],
            1,
            0,
            BlockThresholds::default(),
            vec![BlockOperator::Map {
                exprs: vec![key],
                projections: None,
            }],
            ClusterStatsLayout::Linear,
            vec![
                DataField::new("a", DataType::Number(NumberDataType::Int64)),
                DataField::new("a % 3", DataType::Number(NumberDataType::Int64)),
            ],
            FunctionContext::default(),
        );
        let source = NumberType::<i64>::from_data(vec![1, 2, 3, 4]);
        let block = DataBlock::new_from_columns(vec![source.clone()]);

        let expression_columns = materialize_cluster_key_columns(&block, &generator, Some(vec![1]))
            .unwrap()
            .unwrap();
        assert_eq!(expression_columns, vec![NumberType::<i64>::from_data(
            vec![1, 2, 0, 1]
        )]);

        let direct_columns = materialize_cluster_key_columns(&block, &generator, Some(vec![0]))
            .unwrap()
            .unwrap();
        assert_eq!(direct_columns, vec![source]);
    }
}
