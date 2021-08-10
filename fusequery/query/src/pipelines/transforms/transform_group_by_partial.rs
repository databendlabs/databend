// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodKind;
use common_datavalues::arrays::BinaryArrayBuilder;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_infallible::RwLock;
use common_io::prelude::*;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct GroupByPartialTransform {
    aggr_exprs: Vec<Expression>,
    group_exprs: Vec<Expression>,

    schema: DataSchemaRef,
    schema_before_group_by: DataSchemaRef,
    input: Arc<dyn Processor>,
}

impl GroupByPartialTransform {
    pub fn create(
        schema: DataSchemaRef,
        schema_before_group_by: DataSchemaRef,
        aggr_exprs: Vec<Expression>,
        group_exprs: Vec<Expression>,
    ) -> Self {
        Self {
            aggr_exprs,
            group_exprs,
            schema,
            schema_before_group_by,
            input: Arc::new(EmptyProcessor::create()),
        }
    }
}

#[async_trait::async_trait]
impl Processor for GroupByPartialTransform {
    fn name(&self) -> &str {
        "GroupByPartialTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Create hash group based on row index and apply the function with vector.
    /// For example:
    /// row_idx, A
    /// 0, 1
    /// 1, 2
    /// 2, 3
    /// 3, 4
    /// 4, 5
    ///
    /// grouping by [A%3]
    /// 1.1)
    /// For each row, allocate the state if key not exists in the map, and apply accumulate_row row by row
    ///  row_idx, A % 3 -> state place
    ///  0, 1 -> state1
    ///  1, 2 -> state2
    ///  2, 3 -> state3
    ///  3, 1 -> state1
    ///  4, 2 -> state2
    /// 1.2)  serialize the state to the output block
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");

        let aggr_len = self.aggr_exprs.len();
        let start = Instant::now();
        let schema_before_group_by = self.schema_before_group_by.clone();
        let mut funcs = Vec::with_capacity(self.aggr_exprs.len());
        let mut arg_names = Vec::with_capacity(self.aggr_exprs.len());
        let mut aggr_cols = Vec::with_capacity(self.aggr_exprs.len());

        for expr in self.aggr_exprs.iter() {
            funcs.push(expr.to_aggregate_function(&schema_before_group_by)?);
            arg_names.push(expr.to_aggregate_function_names()?);
            aggr_cols.push(expr.column_name());
        }
        let group_cols = self
            .group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>();

        let mut stream = self.input.execute().await?;
        let arena = Bump::new();
        let sample_block = DataBlock::empty_with_schema(self.schema_before_group_by.clone());
        let method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;

        macro_rules! apply {
            ($hash_method: ident, $key_array_builder: ty, $group_func_table: ty) => {{
                // Table for <group_key, (place, keys) >
                type GroupFuncTable = $group_func_table;
                let groups_locker = GroupFuncTable::default();
                while let Some(block) = stream.next().await {
                    let block = block?;
                    // 1.1 and 1.2.
                    let mut group_columns = Vec::with_capacity(group_cols.len());
                    {
                        for col in group_cols.iter() {
                            group_columns.push(block.try_column_by_name(col)?);
                        }
                    }

                    let mut aggr_arg_columns = Vec::with_capacity(aggr_cols.len());
                    for (idx, _aggr_col) in aggr_cols.iter().enumerate() {
                        let arg_columns = arg_names[idx]
                            .iter()
                            .map(|arg| block.try_column_by_name(arg).and_then(|c| c.to_array()))
                            .collect::<Result<Vec<Series>>>()?;
                        aggr_arg_columns.push(arg_columns);
                    }

                    let group_keys = $hash_method.build_keys(&group_columns, block.num_rows())?;
                    let mut groups = groups_locker.write();
                    {
                        for (row, group_key) in group_keys.iter().enumerate() {
                            match groups.get(group_key) {
                                // New group.
                                None => {
                                    let mut places = Vec::with_capacity(aggr_cols.len());
                                    for (idx, arg_columns) in aggr_arg_columns.iter().enumerate() {
                                        let place = funcs[idx].allocate_state(&arena);
                                        funcs[idx].accumulate_row(place, row, arg_columns)?;
                                        places.push(place);
                                    }
                                    groups.insert(group_key.clone(), places);
                                }
                                // Accumulate result against the take block by indices.
                                Some(places) => {
                                    for (idx, arg_columns) in aggr_arg_columns.iter().enumerate() {
                                        funcs[idx].accumulate_row(places[idx], row, arg_columns)?;
                                    }
                                }
                            }
                        }
                    }
                }

                let delta = start.elapsed();
                tracing::debug!("Group by partial cost: {:?}", delta);

                let groups = groups_locker.read();
                if groups.is_empty() {
                    return Ok(Box::pin(DataBlockStream::create(
                        DataSchemaRefExt::create(vec![]),
                        None,
                        vec![],
                    )));
                }

                // Builders.
                let mut state_builders: Vec<BinaryArrayBuilder> = (0..aggr_len)
                    .map(|_| BinaryArrayBuilder::with_capacity(groups.len() * 4))
                    .collect();

                type KeyBuilder = $key_array_builder;
                let mut group_key_builder = KeyBuilder::with_capacity(groups.len());

                let mut bytes = BytesMut::new();
                for (key, places) in groups.iter() {
                    for (idx, func) in funcs.iter().enumerate() {
                        func.serialize(places[idx], &mut bytes)?;
                        state_builders[idx].append_value(&bytes[..]);
                        bytes.clear();
                    }

                    group_key_builder.append_value((*key).clone());
                }

                let mut columns: Vec<Series> = Vec::with_capacity(self.schema.fields().len());
                for mut builder in state_builders {
                    columns.push(builder.finish().into_series());
                }
                let array = group_key_builder.finish();
                columns.push(array.into_series());

                let block = DataBlock::create_by_array(self.schema.clone(), columns);
                Ok(Box::pin(DataBlockStream::create(
                    self.schema.clone(),
                    None,
                    vec![block],
                )))
            }};
        }

        macro_rules! match_hash_method_and_apply {
            ($method: ident, $apply: ident) => {{
                match $method {
                    HashMethodKind::Serializer(hash_method) => {
                        apply! { hash_method, BinaryArrayBuilder , RwLock<HashMap<Vec<u8>, Vec<usize>, ahash::RandomState>>}
                    }
                    HashMethodKind::KeysU8(hash_method) => {
                        apply! { hash_method , DFUInt8ArrayBuilder, RwLock<HashMap<u8, Vec<usize>, ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU16(hash_method) => {
                        apply! { hash_method , DFUInt16ArrayBuilder, RwLock<HashMap<u16, Vec<usize>, ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU32(hash_method) => {
                        apply! { hash_method , DFUInt32ArrayBuilder, RwLock<HashMap<u32, Vec<usize>, ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU64(hash_method) => {
                        apply! { hash_method , DFUInt64ArrayBuilder, RwLock<HashMap<u64, Vec<usize>, ahash::RandomState>> }
                    }
                }
            }};
        }

        match_hash_method_and_apply! {method, apply}
    }
}
