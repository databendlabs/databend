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
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::packet::JoinRuntimeFilterPacket;
use super::packet::RuntimeFilterPacket;
use super::packet::SerializableDomain;
use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::hash_join::util::hash_by_method_for_bloom;

struct JoinRuntimeFilterPacketBuilder<'a> {
    build_key_column: Column,
    func_ctx: &'a FunctionContext,
    inlist_threshold: usize,
    bloom_threshold: usize,
    min_max_threshold: usize,
    selectivity_threshold: u64,
}

impl<'a> JoinRuntimeFilterPacketBuilder<'a> {
    fn new(
        data_blocks: &'a [DataBlock],
        func_ctx: &'a FunctionContext,
        build_key: &Expr,
        inlist_threshold: usize,
        bloom_threshold: usize,
        min_max_threshold: usize,
        selectivity_threshold: u64,
    ) -> Result<Self> {
        let build_key_column = Self::eval_build_key_column(data_blocks, func_ctx, build_key)?;
        Ok(Self {
            func_ctx,
            build_key_column,
            inlist_threshold,
            bloom_threshold,
            min_max_threshold,
            selectivity_threshold,
        })
    }
    fn build(&self, desc: &RuntimeFilterDesc) -> Result<RuntimeFilterPacket> {
        if !should_enable_runtime_filter(
            desc,
            self.build_key_column.len(),
            self.selectivity_threshold,
        ) {
            return Ok(RuntimeFilterPacket {
                id: desc.id,
                inlist: None,
                min_max: None,
                bloom: None,
            });
        }
        let start = Instant::now();

        let min_max_start = Instant::now();
        let min_max = self
            .enable_min_max(desc)
            .then(|| self.build_min_max())
            .transpose()?;
        let min_max_time = min_max_start.elapsed();

        let inlist_start = Instant::now();
        let inlist = self
            .enable_inlist(desc)
            .then(|| self.build_inlist())
            .transpose()?;
        let inlist_time = inlist_start.elapsed();

        let bloom_start = Instant::now();
        let bloom = self
            .enable_bloom(desc)
            .then(|| self.build_bloom(desc))
            .transpose()?;
        let bloom_time = bloom_start.elapsed();

        let total_time = start.elapsed();

        Profile::record_usize_profile(
            ProfileStatisticsName::RuntimeFilterBuildTime,
            total_time.as_nanos() as usize,
        );

        log::info!(
            "RUNTIME-FILTER: Built filter {} - total: {:?}, min_max: {:?}, inlist: {:?}, bloom: {:?}, rows: {}",
            desc.id,
            total_time,
            min_max_time,
            inlist_time,
            bloom_time,
            self.build_key_column.len()
        );

        Ok(RuntimeFilterPacket {
            id: desc.id,
            min_max,
            inlist,
            bloom,
        })
    }

    fn enable_min_max(&self, desc: &RuntimeFilterDesc) -> bool {
        desc.enable_min_max_runtime_filter && self.build_key_column.len() < self.min_max_threshold
    }

    fn enable_inlist(&self, desc: &RuntimeFilterDesc) -> bool {
        desc.enable_inlist_runtime_filter && self.build_key_column.len() < self.inlist_threshold
    }

    fn enable_bloom(&self, desc: &RuntimeFilterDesc) -> bool {
        if !desc.enable_bloom_runtime_filter {
            return false;
        }

        if self.build_key_column.len() >= self.bloom_threshold {
            return false;
        }

        true
    }

    fn build_min_max(&self) -> Result<SerializableDomain> {
        let domain = self.build_key_column.remove_nullable().domain();
        let (min, max) = domain.to_minmax();
        Ok(SerializableDomain { min, max })
    }

    fn build_inlist(&self) -> Result<Column> {
        self.dedup_column(&self.build_key_column)
    }

    fn build_bloom(&self, desc: &RuntimeFilterDesc) -> Result<Vec<u64>> {
        let data_type = desc.build_key.data_type();
        let num_rows = self.build_key_column.len();
        let method = DataBlock::choose_hash_method_with_types(&[data_type.clone()])?;
        let mut hashes = Vec::with_capacity(num_rows);
        let key_columns = &[self.build_key_column.clone().into()];
        hash_by_method_for_bloom(&method, key_columns.into(), num_rows, &mut hashes)?;
        Ok(hashes)
    }

    fn eval_build_key_column(
        data_blocks: &[DataBlock],
        func_ctx: &FunctionContext,
        build_key: &Expr,
    ) -> Result<Column> {
        let mut columns = Vec::with_capacity(data_blocks.len());
        for block in data_blocks.iter() {
            let evaluator = Evaluator::new(block, func_ctx, &BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(build_key)?
                .convert_to_full_column(build_key.data_type(), block.num_rows());
            columns.push(column);
        }
        Column::concat_columns(columns.into_iter())
    }

    fn dedup_column(&self, column: &Column) -> Result<Column> {
        let array = RawExpr::Constant {
            span: None,
            scalar: Scalar::Array(column.clone()),
            data_type: Some(DataType::Array(Box::new(column.data_type()))),
        };
        let distinct_list = RawExpr::FunctionCall {
            span: None,
            name: "array_distinct".to_string(),
            params: vec![],
            args: vec![array],
        };

        let empty_key_block = DataBlock::empty();
        let evaluator = Evaluator::new(&empty_key_block, self.func_ctx, &BUILTIN_FUNCTIONS);
        let value = evaluator.run(&type_check::check(&distinct_list, &BUILTIN_FUNCTIONS)?)?;
        let array = value.into_scalar().unwrap().into_array().unwrap();
        Ok(array)
    }
}

pub(super) fn should_enable_runtime_filter(
    desc: &RuntimeFilterDesc,
    build_num_rows: usize,
    selectivity_threshold: u64,
) -> bool {
    if build_num_rows == 0 {
        return false;
    }

    let Some(build_table_rows) = desc.build_table_rows else {
        log::info!(
            "RUNTIME-FILTER: Disable runtime filter {} - no build table statistics available",
            desc.id
        );
        return false;
    };

    let selectivity_pct = (build_num_rows as f64 / build_table_rows as f64) * 100.0;

    if selectivity_pct < selectivity_threshold as f64 {
        log::info!(
            "RUNTIME-FILTER: Enable runtime filter {} - low selectivity: {:.2}% < {}% (build_rows={}, build_table_rows={})",
            desc.id,
            selectivity_pct,
            selectivity_threshold,
            build_num_rows,
            build_table_rows
        );
        true
    } else {
        log::info!(
            "RUNTIME-FILTER: Disable runtime filter {} - high selectivity: {:.2}% >= {}% (build_rows={}, build_table_rows={})",
            desc.id,
            selectivity_pct,
            selectivity_threshold,
            build_num_rows,
            build_table_rows
        );
        false
    }
}

pub fn build_runtime_filter_packet(
    build_chunks: &[DataBlock],
    build_num_rows: usize,
    runtime_filter_desc: &[RuntimeFilterDesc],
    func_ctx: &FunctionContext,
    inlist_threshold: usize,
    bloom_threshold: usize,
    min_max_threshold: usize,
    selectivity_threshold: u64,
    is_spill_happened: bool,
) -> Result<JoinRuntimeFilterPacket> {
    if is_spill_happened {
        return Ok(JoinRuntimeFilterPacket::disable_all(build_num_rows));
    }
    if build_num_rows == 0 {
        return Ok(JoinRuntimeFilterPacket::complete_without_filters(
            build_num_rows,
        ));
    }
    let mut runtime_filters = HashMap::new();
    for rf in runtime_filter_desc {
        runtime_filters.insert(
            rf.id,
            JoinRuntimeFilterPacketBuilder::new(
                build_chunks,
                func_ctx,
                &rf.build_key,
                inlist_threshold,
                bloom_threshold,
                min_max_threshold,
                selectivity_threshold,
            )?
            .build(rf)?,
        );
    }
    Ok(JoinRuntimeFilterPacket::complete(
        runtime_filters,
        build_num_rows,
    ))
}
