// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_aggregate_functions::AggregateFunction;
use common_arrow::arrow::array::BinaryBuilder;
use common_arrow::arrow::array::StringBuilder;
use common_datablocks::DataBlock;
use common_datavalues::DataArrayRef;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataValue;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use log::info;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

// Table for <group_key, ((function, column_name, args), keys) >
type GroupFuncTable = RwLock<
    HashMap<
        Vec<u8>,
        (
            Vec<(Box<dyn AggregateFunction>, String, Vec<String>)>,
            Vec<DataValue>,
        ),
        ahash::RandomState,
    >,
>;

pub struct GroupByPartialTransform {
    aggr_exprs: Vec<Expression>,
    group_exprs: Vec<Expression>,
    schema: DataSchemaRef,
    schema_before_groupby: DataSchemaRef,
    input: Arc<dyn Processor>,
    groups: GroupFuncTable,
}

impl GroupByPartialTransform {
    pub fn create(
        schema: DataSchemaRef,
        schema_before_groupby: DataSchemaRef,
        aggr_exprs: Vec<Expression>,
        group_exprs: Vec<Expression>,
    ) -> Self {
        Self {
            aggr_exprs,
            group_exprs,
            schema,
            schema_before_groupby,
            input: Arc::new(EmptyProcessor::create()),
            groups: RwLock::new(HashMap::default()),
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
    /// row_idx, group_key, A
    /// 0, 1, 1
    /// 1, 2, 2
    /// 2, 0, 3
    /// 3, 1, 4
    /// 4, 2, 5
    ///
    /// 1.2) make indices group(for vector compute)
    /// group_key, indices
    /// 0, [2]
    /// 1, [0, 3]
    /// 2, [1, 4]
    ///
    /// 1.3) apply aggregate function(SUM(A)) to the take block
    /// group_key, SUM(A)
    /// <0, 3>
    /// <1, 1+4>
    /// <2, 2+5>
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let aggr_len = self.aggr_exprs.len();
        let start = Instant::now();
        let schema_before_groupby = self.schema_before_groupby.clone();

        let mut stream = self.input.execute().await?;

        while let Some(block) = stream.next().await {
            let block = block?;
            let cols = self
                .group_exprs
                .iter()
                .map(|x| x.column_name())
                .collect::<Vec<_>>();

            // 1.1 and 1.2.
            let group_blocks = DataBlock::group_by(&block, &cols)?;
            // 1.3 Apply take blocks to aggregate function by group_key.
            {
                for (group_key, group_keys, take_block) in group_blocks {
                    let rows = take_block.num_rows();

                    let mut groups = self.groups.write();
                    match groups.get_mut(&group_key) {
                        // New group.
                        None => {
                            let mut aggr_funcs = vec![];
                            for expr in &self.aggr_exprs {
                                let mut func =
                                    expr.to_aggregate_function(&schema_before_groupby)?;
                                let name = expr.column_name();
                                let args = expr.to_aggregate_function_names()?;
                                let arg_columns = args
                                    .iter()
                                    .map(|arg| {
                                        take_block.try_column_by_name(arg).map(|c| c.clone())
                                    })
                                    .collect::<Result<Vec<DataColumnarValue>>>()?;
                                func.accumulate(&arg_columns, rows)?;
                                aggr_funcs.push((func, name, args));
                            }

                            groups.insert(group_key.clone(), (aggr_funcs, group_keys));
                        }
                        // Accumulate result against the take block by indices.
                        Some((aggr_funcs, _)) => {
                            for func in aggr_funcs {
                                let arg_columns = func
                                    .2
                                    .iter()
                                    .map(|arg| {
                                        take_block.try_column_by_name(arg).map(|c| c.clone())
                                    })
                                    .collect::<Result<Vec<DataColumnarValue>>>()?;

                                func.0.accumulate(&arg_columns, rows)?
                            }
                        }
                    }
                }
            }
        }

        if self.groups.read().is_empty() {
            return Ok(Box::pin(DataBlockStream::create(
                DataSchemaRefExt::create(vec![]),
                None,
                vec![],
            )));
        }

        let delta = start.elapsed();
        info!("Group by partial cost: {:?}", delta);

        let groups = self.groups.read();

        // Builders.
        let mut builders: Vec<StringBuilder> = (0..1 + aggr_len)
            .map(|_| StringBuilder::new(groups.len()))
            .collect();

        let mut group_key_builder = BinaryBuilder::new(groups.len());
        for (key, (funcs, values)) in groups.iter() {
            for (idx, func) in funcs.iter().enumerate() {
                let states = DataValue::Struct(func.0.accumulate_result()?);
                let ser = serde_json::to_string(&states)?;
                builders[idx].append_value(ser.as_str())?;
            }

            // TODO: separate keys in each column
            let key_ser = serde_json::to_string(&DataValue::Struct(values.clone()))?;
            builders[aggr_len].append_value(key_ser.as_str())?;

            group_key_builder.append_value(key)?;
        }

        let mut columns: Vec<DataArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for mut builder in builders {
            columns.push(Arc::new(builder.finish()));
        }
        columns.push(Arc::new(group_key_builder.finish()));

        let block = DataBlock::create_by_array(self.schema.clone(), columns);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
