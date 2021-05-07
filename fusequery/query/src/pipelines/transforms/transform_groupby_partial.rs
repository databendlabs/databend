// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow::array::BinaryBuilder;
use common_arrow::arrow::array::StringBuilder;
use common_datablocks::DataBlock;
use common_datavalues::DataArrayRef;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::IFunction;
use common_infallible::RwLock;
use common_planners::ExpressionPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use log::info;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

// Table for <group_key, indices>
type GroupIndicesTable = HashMap<Vec<u8>, Vec<u32>, ahash::RandomState>;
type GroupFuncTable = RwLock<HashMap<Vec<u8>, Vec<Box<dyn IFunction>>, ahash::RandomState>>;

pub struct GroupByPartialTransform {
    aggr_exprs: Vec<ExpressionPlan>,
    group_exprs: Vec<ExpressionPlan>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
    groups: GroupFuncTable
}

impl GroupByPartialTransform {
    pub fn create(
        schema: DataSchemaRef,
        aggr_exprs: Vec<ExpressionPlan>,
        group_exprs: Vec<ExpressionPlan>
    ) -> Self {
        Self {
            aggr_exprs,
            group_exprs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
            groups: RwLock::new(HashMap::default())
        }
    }
}

#[async_trait::async_trait]
impl IProcessor for GroupByPartialTransform {
    fn name(&self) -> &str {
        "GroupByPartialTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
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
    /// row_idx, group_key
    /// 0, 1
    /// 1, 2
    /// 2, 0
    /// 3, 1
    /// 4, 2
    ///
    /// 1.2) make indices group(for vector compute)
    /// group_key, indices
    /// 0, [2]
    /// 1, [0, 3]
    /// 2, [1, 4]
    ///
    /// 1.3) apply aggregate function(SUM(A)) to the take block
    /// group_key, SUM(A)
    /// 0, 3
    /// 1, 5
    /// 2, 7
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let group_funcs = self
            .group_exprs
            .iter()
            .map(|x| x.to_function())
            .collect::<common_exception::Result<Vec<_>>>()?;
        let group_funcs_len = group_funcs.len();
        let aggr_funcs_len = self.aggr_exprs.len();

        let start = Instant::now();
        let mut stream = self.input.execute().await?;
        while let Some(block) = stream.next().await {
            let block = block?;
            let mut group_indices = GroupIndicesTable::default();
            let mut group_columns = Vec::with_capacity(group_funcs_len);

            // 1.1 Eval the group expr columns.
            {
                for func in &group_funcs {
                    let group_column = func.eval(&block)?.to_array(block.num_rows())?;
                    group_columns.push(group_column);
                }
            }

            // 1.2 Make group with indices.
            {
                let mut group_key_len = 0;
                for col in &group_columns {
                    let typ = col.data_type();
                    if common_datavalues::is_integer(typ) {
                        group_key_len += common_datavalues::numeric_byte_size(typ)?;
                    } else {
                        group_key_len += 4;
                    }
                }

                let mut group_key = Vec::with_capacity(group_key_len);
                for row in 0..block.num_rows() {
                    group_key.clear();
                    for col in &group_columns {
                        DataValue::concat_row_to_one_key(col, row, &mut group_key)?;
                    }
                    match group_indices.get_mut(&group_key) {
                        None => {
                            group_indices.insert(group_key.clone(), vec![row as u32]);
                        }
                        Some(v) => {
                            v.push(row as u32);
                        }
                    }
                }
            }

            // 1.3 Apply take blocks to aggregate function by group_key.
            {
                for (group_key, group_indices) in group_indices {
                    let take_block = DataBlock::block_take_by_indices(&block, &group_indices)?;
                    let mut groups = self.groups.write();
                    match groups.get_mut(&group_key) {
                        // New group.
                        None => {
                            let mut aggr_funcs = Vec::with_capacity(aggr_funcs_len);
                            for expr in &self.aggr_exprs {
                                let mut func = expr.to_function()?;
                                func.accumulate(&take_block)?;
                                aggr_funcs.push(func);
                            }
                            groups.insert(group_key.clone(), aggr_funcs);
                        }
                        // Accumulate result against the take block by indices.
                        Some(aggr_funcs) => {
                            for func in aggr_funcs {
                                func.accumulate(&take_block)?
                            }
                        }
                    }
                }
            }
        }
        let delta = start.elapsed();
        info!("Group by partial cost: {:?}", delta);

        let groups = self.groups.read();

        // Fields.
        let mut fields = Vec::with_capacity(aggr_funcs_len + 1);
        if let Some((_, funcs)) = groups.iter().next() {
            for func in funcs {
                let field = DataField::new(format!("{}", func).as_str(), DataType::Utf8, false);
                fields.push(field);
            }
        }
        fields.push(DataField::new("_group_by_key", DataType::Binary, false));

        // Builders.
        let mut builders: Vec<StringBuilder> = (0..fields.len() - 1)
            .map(|_| StringBuilder::new(groups.len()))
            .collect();
        let mut group_key_builder = BinaryBuilder::new(groups.len());
        for (key, funcs) in groups.iter() {
            for (idx, func) in funcs.iter().enumerate() {
                let states = DataValue::Struct(func.accumulate_result()?);
                let ser = serde_json::to_string(&states).map_err(ErrorCodes::from_serde)?;
                builders[idx]
                    .append_value(ser.as_str())
                    .map_err(ErrorCodes::from_arrow)?;
            }
            group_key_builder
                .append_value(key)
                .map_err(ErrorCodes::from_arrow)?;
        }

        let mut columns: Vec<DataArrayRef> = Vec::with_capacity(fields.len());
        for mut builder in builders {
            columns.push(Arc::new(builder.finish()));
        }
        columns.push(Arc::new(group_key_builder.finish()));

        let schema = DataSchemaRefExt::create(fields);
        let block = DataBlock::create(schema, columns);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block]
        )))
    }
}
