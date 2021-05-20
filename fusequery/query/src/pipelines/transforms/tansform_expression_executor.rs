// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::ActionNode;
use common_planners::ExpressionAction;
use common_planners::ExpressionChain;

#[derive(Clone, Debug)]
pub struct ExpressionExecutor {
    schema: DataSchemaRef,
    exprs: Vec<ExpressionAction>,
    chain: ExpressionChain,
    // whether to perform alias action in executor
    alias_project: bool
}

impl ExpressionExecutor {
    pub fn try_create(
        schema: DataSchemaRef,
        exprs: Vec<ExpressionAction>,
        alias_project: bool
    ) -> Result<Self> {
        let chain = ExpressionChain::try_create(schema.clone(), &exprs)?;

        Ok(Self {
            schema,
            exprs,
            chain,
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

        self.chain.nodes.iter().for_each(|action| {
            match action {
                ActionNode::Input(input) => {
                    let column = block.try_column_by_name(&input.name)?;
                    column_map.insert(column_name.clone(), column.clone());
                    columns.push(column);
                }
                ActionNode::Function(f) => {
                    // check if it's cached
                    if !column_map.contains(&f.name) {
                        let arg_columns = f
                            .arg_names
                            .iter()
                            .map(|arg| {
                                column_map.get(arg).ok_or_else(Err(ErrorCodes::LogicalError(
                                    "Arguments must be prepared before function transform"
                                )))
                            })
                            .collect::<Vec<Result<DataColumnarValue>>>()?;

                        let func = f.to_function()?;
                        let column = func.eval(&arg_columns, rows)?;
                        column_map.insert(f.name.clone(), column.clone());
                        columns.push(column);
                    }
                }
                // we just ignore alias action in expressions
                ActionNode::Alias(alias) => {
                    if self.alias_project {
                        let column = column_map.get(&alias.arg_name).ok_or_else(
                            ErrorCodes::LogicalError(
                                "ActionNode ALIAS must have column to transform"
                            )
                        )?;

                        column_map.insert(alias.name.clone(), column.clone());
                        column_map.put(&alias.name, column);
                    }
                }
                ActionNode::Constant(constant) => {
                    if column_map.contains_key(&constant.name) {
                        let column = DataColumnarValue::Constant(constant.value, rows);
                        column_map.insert(alias.name.clone(), column.clone());
                        column_map.put(&alias.name, column);
                    }
                }
            }
        });

        let mut project_columns = Vec::with_capacity(self.exprs.len());
        self.exprs.iter().for_each(|expr| {
            project_columns.push(
                column_map
                    .get(&expr.column_name())
                    .ok_or_else(ErrorCodes::LogicalError(format!(
                        "Column {} must be prepared in upstream",
                        expr.column_name()
                    )))?
                    .to_array()?
            );
        });

        // projection to remove unused columns
        Ok(DataBlock::create(self.schema.clone(), project_columns))
    }
}
