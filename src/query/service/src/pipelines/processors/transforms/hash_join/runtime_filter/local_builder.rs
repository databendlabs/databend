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

use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::RuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::SerializableDomain;
use crate::pipelines::processors::transforms::hash_join::util::hash_by_method_for_bloom;

struct SingleFilterBuilder {
    id: usize,
    data_type: DataType,
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
        let hash_method = DataBlock::choose_hash_method_with_types(&[data_type.clone()])?;

        Ok(Self {
            id: desc.id,
            data_type,
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

    fn add_column(&mut self, column: &Column, total_rows: usize) -> Result<()> {
        let new_total = total_rows + column.len();
        self.add_min_max(column, new_total);
        self.add_inlist(column, new_total);
        self.add_bloom(column, new_total)?;
        Ok(())
    }

    fn add_min_max(&mut self, column: &Column, new_total: usize) {
        if new_total > self.min_max_threshold {
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
            self.inlist_builder = None;
            return;
        }
        let mut builder = match self.inlist_builder.take() {
            Some(b) => b,
            None => ColumnBuilder::with_capacity(&self.data_type, column.len()),
        };
        builder.append_column(column);
        self.inlist_builder = Some(builder);
    }

    fn add_bloom(&mut self, column: &Column, new_total: usize) -> Result<()> {
        if new_total > self.bloom_threshold {
            self.bloom_hashes = None;
            return Ok(());
        }
        let mut hashes = match self.bloom_hashes.take() {
            Some(h) => h,
            None => Vec::with_capacity(column.len()),
        };
        hashes.reserve(column.len());
        let entry = BlockEntry::from(column.clone());
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
                Some(dedup_column(column, func_ctx, &self.data_type)?)
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
        }))
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);

        for (builder, desc) in self.builders.iter_mut().zip(self.runtime_filters.iter()) {
            let column = evaluator
                .run(&desc.build_key)?
                .convert_to_full_column(desc.build_key.data_type(), block.num_rows());
            builder.add_column(&column, self.total_rows)?;
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
    let block = DataBlock::empty();
    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let value = evaluator.run(&expr)?;

    Ok(value.into_scalar().unwrap().into_array().unwrap())
}
