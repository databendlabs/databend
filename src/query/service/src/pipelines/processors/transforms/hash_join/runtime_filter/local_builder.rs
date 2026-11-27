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

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::builder::bloom_selectivity_allows;
use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::RuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::SerializableDomain;
use crate::pipelines::processors::transforms::hash_join::util::hash_by_method_for_bloom;

struct SingleFilterBuilder {
    id: usize,
    inlist_data_type: DataType,
    hash_method: HashMethodKind,

    min_max_domain: Option<Domain>,
    min_max_threshold: usize,

    inlist_builder: Option<ColumnBuilder>,
    inlist_threshold: usize,

    bloom_hashes: Option<Vec<u64>>,
    bloom_threshold: usize,
}

impl SingleFilterBuilder {
    fn new(
        desc: &RuntimeFilterDesc,
        inlist_threshold: usize,
        bloom_threshold: usize,
        min_max_threshold: usize,
    ) -> Result<Self> {
        let data_type = desc.build_key.data_type().clone();
        let bloom_data_type = data_type.remove_nullable();
        let hash_method = DataBlock::choose_hash_method_with_types(&[bloom_data_type.clone()])?;

        Ok(Self {
            id: desc.id,
            inlist_data_type: bloom_data_type,
            hash_method,
            min_max_domain: None,
            min_max_threshold: if desc.enable_min_max_runtime_filter {
                min_max_threshold
            } else {
                0
            },
            inlist_builder: None,
            inlist_threshold: if desc.enable_inlist_runtime_filter {
                inlist_threshold
            } else {
                0
            },
            bloom_hashes: None,
            bloom_threshold: if desc.enable_bloom_runtime_filter {
                bloom_threshold
            } else {
                0
            },
        })
    }

    /// `rows_before` counts the rows already added before `column`. Each threshold below is
    /// compared against `rows_before + column.len()`, so a filter part is dropped once that
    /// count exceeds the threshold the merge applies to the total build rows.
    ///
    /// A dropped part stays dropped: its threshold is set to 0. Restarting it from a later block
    /// would build a filter that misses the earlier rows, and prune probe rows matching them.
    fn add_column(
        &mut self,
        column: &Column,
        rows_before: usize,
        bloom_allowed: bool,
    ) -> Result<()> {
        let new_total = rows_before + column.len();
        self.add_min_max(column, new_total);
        self.add_inlist(column, new_total);
        self.add_bloom(column, new_total, bloom_allowed)?;
        Ok(())
    }

    fn add_min_max(&mut self, column: &Column, new_total: usize) {
        if new_total > self.min_max_threshold {
            self.min_max_threshold = 0;
            self.min_max_domain = None;
            return;
        }
        let col_domain = column.remove_nullable().domain();
        self.min_max_domain = Some(match self.min_max_domain.take() {
            Some(d) => d.merge(&col_domain),
            None => col_domain,
        });
    }

    fn add_inlist(&mut self, column: &Column, new_total: usize) {
        if new_total > self.inlist_threshold {
            self.inlist_threshold = 0;
            self.inlist_builder = None;
            return;
        }
        let column = column.remove_nullable();
        let mut builder = match self.inlist_builder.take() {
            Some(b) => b,
            None => ColumnBuilder::with_capacity(&self.inlist_data_type, column.len()),
        };
        builder.append_column(&column);
        self.inlist_builder = Some(builder);
    }

    fn add_bloom(&mut self, column: &Column, new_total: usize, bloom_allowed: bool) -> Result<()> {
        if !bloom_allowed || new_total > self.bloom_threshold {
            self.bloom_threshold = 0;
            self.bloom_hashes = None;
            return Ok(());
        }
        let mut hashes = match self.bloom_hashes.take() {
            Some(h) => h,
            None => Vec::with_capacity(column.len()),
        };
        hashes.reserve(column.len());
        let entry = BlockEntry::from(column.remove_nullable());
        hash_by_method_for_bloom(
            &self.hash_method,
            (&[entry]).into(),
            column.len(),
            &mut hashes,
        )?;
        self.bloom_hashes = Some(hashes);
        Ok(())
    }

    fn finish(mut self, func_ctx: &FunctionContext) -> Result<RuntimeFilterPacket> {
        let min_max = self.min_max_domain.take().map(|domain| {
            let (min, max) = domain.to_minmax();
            SerializableDomain { min, max }
        });

        let inlist = if let Some(builder) = self.inlist_builder.take() {
            let column = builder.build();
            if column.len() == 0 {
                None
            } else {
                Some(dedup_column(column, func_ctx, &self.inlist_data_type)?)
            }
        } else {
            None
        };

        let bloom = self.bloom_hashes.take();

        Ok(RuntimeFilterPacket {
            id: self.id,
            min_max,
            inlist,
            bloom,
        })
    }
}

pub struct RuntimeFilterLocalBuilder {
    func_ctx: FunctionContext,
    builders: Vec<SingleFilterBuilder>,
    total_rows: usize,
    runtime_filters: Vec<RuntimeFilterDesc>,
    bloom_selectivity_threshold: Option<u64>,
}

impl RuntimeFilterLocalBuilder {
    pub fn try_create(
        func_ctx: &FunctionContext,
        descs: Vec<RuntimeFilterDesc>,
        inlist_threshold: usize,
        bloom_threshold: usize,
        min_max_threshold: usize,
    ) -> Result<Option<Self>> {
        if descs.is_empty() {
            return Ok(None);
        }

        let mut builders = Vec::with_capacity(descs.len());
        for desc in descs.iter() {
            builders.push(SingleFilterBuilder::new(
                desc,
                inlist_threshold,
                bloom_threshold,
                min_max_threshold,
            )?);
        }

        Ok(Some(Self {
            func_ctx: func_ctx.clone(),
            builders,
            total_rows: 0,
            runtime_filters: descs,
            bloom_selectivity_threshold: None,
        }))
    }

    /// Also drop bloom hashes once the build rows rule out the bloom filter by selectivity
    /// (see `should_enable_runtime_filter`), instead of only when the packet is converted.
    pub fn with_bloom_selectivity_threshold(mut self, selectivity_threshold: u64) -> Self {
        self.bloom_selectivity_threshold = Some(selectivity_threshold);
        self
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        self.add_block_after(block, self.total_rows)
    }

    /// Add a block whose rows come after `rows_before` build rows of the same join.
    ///
    /// When several local builders feed one merged packet, passing the rows already added by
    /// all of them (instead of this builder's own rows) lets each builder release the bloom
    /// hashes and inlist values as soon as the merge is bound to drop them. The thresholds and
    /// the selectivity check are monotone in the row count, so this never drops a filter part
    /// that the merge of the complete build would keep.
    pub fn add_block_after(&mut self, block: &DataBlock, rows_before: usize) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        debug_assert!(rows_before >= self.total_rows);
        let new_total = rows_before + block.num_rows();
        let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);

        for (builder, desc) in self.builders.iter_mut().zip(self.runtime_filters.iter()) {
            let bloom_allowed = self
                .bloom_selectivity_threshold
                .is_none_or(|threshold| bloom_selectivity_allows(desc, new_total, threshold));
            let column = evaluator
                .run(&desc.build_key)?
                .convert_to_full_column(desc.build_key.data_type(), block.num_rows());
            builder.add_column(&column, rows_before, bloom_allowed)?;
        }

        self.total_rows += block.num_rows();
        Ok(())
    }

    pub fn finish(self, spill_happened: bool) -> Result<JoinRuntimeFilterPacket> {
        let total_rows = self.total_rows;

        if spill_happened {
            return Ok(JoinRuntimeFilterPacket::disable_all(total_rows));
        }

        if total_rows == 0 {
            return Ok(JoinRuntimeFilterPacket::complete_without_filters(0));
        }

        let packets: Vec<_> = self
            .builders
            .into_iter()
            .map(|b| {
                let id = b.id;
                b.finish(&self.func_ctx).map(|p| (id, p))
            })
            .collect::<Result<_>>()?;

        Ok(JoinRuntimeFilterPacket::complete(
            packets.into_iter().collect(),
            total_rows,
        ))
    }
}

fn dedup_column(
    column: Column,
    func_ctx: &FunctionContext,
    data_type: &DataType,
) -> Result<Column> {
    let array_expr = RawExpr::Constant {
        span: None,
        scalar: Scalar::Array(column),
        data_type: Some(DataType::Array(Box::new(data_type.clone()))),
    };
    let distinct_expr = RawExpr::FunctionCall {
        span: None,
        name: "array_distinct".to_string(),
        params: vec![],
        args: vec![array_expr],
    };

    let expr = databend_common_expression::type_check::check(&distinct_expr, &BUILTIN_FUNCTIONS)?;
    let block = DataBlock::empty_with_rows(1);
    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let value = evaluator.run(&expr)?;

    Ok(value.into_scalar().unwrap().into_array().unwrap())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Expr;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;

    use super::*;
    use crate::pipelines::processors::transforms::hash_join::runtime_filter::merge::merge_join_runtime_filter_packets;

    #[test]
    fn test_inlist_runtime_filter_removes_nullable_build_key() -> Result<()> {
        let desc = RuntimeFilterDesc {
            id: 0,
            build_key: Expr::ColumnRef(ColumnRef {
                span: None,
                id: 0,
                data_type: DataType::Nullable(Box::new(DataType::String)),
                display_name: "build_key".to_string(),
            }),
            probe_targets: vec![],
            build_table_rows: Some(2),
            probe_table_rows: None,
            enable_bloom_runtime_filter: false,
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter: false,
        };

        let mut builder = SingleFilterBuilder::new(&desc, 10, 0, 0)?;
        let column = StringType::from_opt_data(vec![Some("a"), Some("b")]);
        builder.add_column(&column, 0, true)?;

        let packet = builder.finish(&FunctionContext::default())?;
        let inlist = packet.inlist.unwrap();
        assert_eq!(inlist.data_type(), DataType::String);
        assert_eq!(inlist.len(), 2);
        Ok(())
    }

    fn int_desc(build_table_rows: Option<u64>) -> RuntimeFilterDesc {
        RuntimeFilterDesc {
            id: 0,
            build_key: Expr::ColumnRef(ColumnRef {
                span: None,
                id: 0,
                data_type: DataType::Number(NumberDataType::Int64),
                display_name: "build_key".to_string(),
            }),
            probe_targets: vec![],
            build_table_rows,
            probe_table_rows: None,
            enable_bloom_runtime_filter: true,
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter: true,
        }
    }

    fn int_block(values: Vec<i64>) -> DataBlock {
        DataBlock::new_from_columns(vec![Int64Type::from_data(values)])
    }

    /// Build the local packets of several builders that share one row count, add them in turn,
    /// and merge them as the join does.
    fn build_shared(
        desc: &RuntimeFilterDesc,
        blocks: Vec<Vec<i64>>,
        selectivity_threshold: Option<u64>,
        shared: bool,
    ) -> Result<(Vec<JoinRuntimeFilterPacket>, JoinRuntimeFilterPacket)> {
        let func_ctx = FunctionContext::default();
        let mut rows = 0;
        let mut packets = vec![];
        for values in blocks {
            let mut builder =
                RuntimeFilterLocalBuilder::try_create(&func_ctx, vec![desc.clone()], 4, 6, 100)?
                    .unwrap();
            if let Some(threshold) = selectivity_threshold {
                builder = builder.with_bloom_selectivity_threshold(threshold);
            }
            let block = int_block(values);
            match shared {
                true => builder.add_block_after(&block, rows)?,
                false => builder.add_block(&block)?,
            }
            rows += block.num_rows();
            packets.push(builder.finish(false)?);
        }
        let merged = merge_join_runtime_filter_packets(packets.clone(), 4, 6, 100)?;
        Ok((packets, merged))
    }

    #[test]
    fn test_shared_rows_drop_filters_the_merge_drops() -> Result<()> {
        let desc = int_desc(None);
        let blocks = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]];

        let (local, local_merged) = build_shared(&desc, blocks.clone(), None, false)?;
        let (shared, shared_merged) = build_shared(&desc, blocks, None, true)?;

        // Builders that only count their own rows keep everything until the merge.
        assert!(
            local
                .iter()
                .all(|p| p.packets.as_ref().unwrap()[&0].bloom.is_some())
        );
        assert!(
            local
                .iter()
                .all(|p| p.packets.as_ref().unwrap()[&0].inlist.is_some())
        );

        // With the shared count, the second builder already passes the inlist threshold (4)
        // and the third the bloom threshold (6).
        let second = &shared[1].packets.as_ref().unwrap()[&0];
        assert!(second.inlist.is_none());
        assert!(second.bloom.is_some());
        let third = &shared[2].packets.as_ref().unwrap()[&0];
        assert!(third.inlist.is_none());
        assert!(third.bloom.is_none());
        assert!(third.min_max.is_some());

        // Dropping early never changes the merged packet.
        assert_eq!(shared_merged, local_merged);
        let merged = &shared_merged.packets.as_ref().unwrap()[&0];
        assert!(merged.bloom.is_none() && merged.inlist.is_none());
        assert!(merged.min_max.is_some());
        assert_eq!(shared_merged.build_rows, 7);
        Ok(())
    }

    #[test]
    fn test_dropped_filter_parts_never_restart() -> Result<()> {
        let desc = int_desc(Some(40));
        let mut builder = SingleFilterBuilder::new(&desc, 4, 6, 6)?;
        let column = |values: Vec<i64>| Int64Type::from_data(values);

        builder.add_column(&column(vec![1, 2, 3, 4, 5]), 0, true)?;
        // 7 rows exceed every threshold, and the bloom filter is not allowed either.
        builder.add_column(&column(vec![6, 7]), 5, false)?;
        // Even with a row count under every threshold, a later block must not start a filter
        // that only holds its own keys.
        builder.add_column(&column(vec![8]), 0, true)?;

        let packet = builder.finish(&FunctionContext::default())?;
        assert!(packet.min_max.is_none());
        assert!(packet.inlist.is_none());
        assert!(packet.bloom.is_none());
        Ok(())
    }

    #[test]
    fn test_shared_rows_drop_bloom_by_selectivity() -> Result<()> {
        // 4 build rows of a 40-row table are not below 10%, 3 rows are.
        let desc = int_desc(Some(40));

        let (shared, merged) = build_shared(&desc, vec![vec![1, 2], vec![3]], Some(10), true)?;
        assert!(
            shared
                .iter()
                .all(|p| p.packets.as_ref().unwrap()[&0].bloom.is_some())
        );
        assert!(merged.packets.as_ref().unwrap()[&0].bloom.is_some());
        assert!(super::super::builder::should_enable_runtime_filter(
            &desc,
            merged.build_rows,
            10
        ));

        let (shared, merged) = build_shared(&desc, vec![vec![1, 2], vec![3, 4]], Some(10), true)?;
        assert!(shared[0].packets.as_ref().unwrap()[&0].bloom.is_some());
        assert!(shared[1].packets.as_ref().unwrap()[&0].bloom.is_none());
        assert!(merged.packets.as_ref().unwrap()[&0].bloom.is_none());
        // The bloom filter dropped early is one the conversion would not enable anyway.
        assert!(!super::super::builder::should_enable_runtime_filter(
            &desc,
            merged.build_rows,
            10
        ));
        Ok(())
    }
}
