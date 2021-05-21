// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::ActionNode;
use common_planners::ExpressionAction;
use common_planners::ExpressionChain;

/// ExpressionExecutor is a helper struct for expressions and projections
/// Aggregate functions is not covered, because all expressions in aggregate functions functions are executed.
#[derive(Debug, Clone)]
pub struct ExpressionExecutor {
    input_schema: DataSchemaRef,
    output_schema: DataSchemaRef,
    chain: Arc<ExpressionChain>,
    // whether to perform alias action in executor
    alias_project: bool
}

impl ExpressionExecutor {
    pub fn try_create(
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        exprs: Vec<ExpressionAction>,
        alias_project: bool
    ) -> Result<Self> {
        let chain = ExpressionChain::try_create(input_schema.clone(), &exprs)?;

        Ok(Self {
            input_schema,
            output_schema,
            chain: Arc::new(chain),
            alias_project
        })
    }

    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    pub fn execute(&self, block: &DataBlock) -> Result<DataBlock> {
        let mut columns = vec![];
        let mut column_map = HashMap::new();
        let rows = block.num_rows();

        for action in self.chain.actions.iter() {
            match action {
                ActionNode::Input(input) => {
                    let column =
                        DataColumnarValue::Array(block.try_column_by_name(&input.name)?.clone());

                    columns.push(column.clone());
                    column_map.insert(input.name.clone(), column);
                }
                ActionNode::Function(f) => {
                    // check if it's cached
                    if !column_map.contains_key(&f.name) {
                        let arg_columns = f
                            .arg_names
                            .iter()
                            .map(|arg| {
                                column_map.get(arg).map(|c| c.clone()).ok_or_else(|| {
                                    ErrorCodes::LogicalError(
                                        "Arguments must be prepared before function transform"
                                    )
                                })
                            })
                            .collect::<Result<Vec<DataColumnarValue>>>()?;

                        let func = f.to_function()?;
                        let column = func.eval(&arg_columns, rows)?;
                        columns.push(column.clone());
                        column_map.insert(f.name.clone(), column);
                    }
                }
                // we just ignore alias action in expressions
                ActionNode::Alias(alias) => {
                    if self.alias_project {
                        let column = column_map.get(&alias.arg_name).ok_or_else(|| {
                            ErrorCodes::LogicalError(
                                "ActionNode ALIAS must have column to transform"
                            )
                        })?;

                        columns.push(column.clone());
                        column_map.insert(alias.name.clone(), column.clone());
                    }
                }
                ActionNode::Constant(constant) => {
                    if column_map.contains_key(&constant.name) {
                        let column = DataColumnarValue::Constant(constant.value.clone(), rows);
                        columns.push(column.clone());
                        column_map.insert(constant.name.clone(), column);
                    }
                }
            }
        }

        let mut project_columns = Vec::with_capacity(self.output_schema.fields().len());
        for f in self.output_schema.fields() {
            let column = column_map.get(f.name()).ok_or_else(|| {
                ErrorCodes::LogicalError("Projection column not exists, there are bugs!")
            })?;
            project_columns.push(column.to_array()?);
        }
        // projection to remove unused columns
        Ok(DataBlock::create(
            self.output_schema.clone(),
            project_columns
        ))
    }
}
