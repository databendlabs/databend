// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ActionFunction;
use common_planners::Expression;
use common_planners::ExpressionAction;
use common_planners::ExpressionChain;
use common_tracing::tracing;

/// ExpressionExecutor is a helper struct for expressions and projections
/// Aggregate functions is not covered, because all expressions in aggregate functions functions are executed.
#[derive(Debug, Clone)]
pub struct ExpressionExecutor {
    // description of this executor
    description: String,
    _input_schema: DataSchemaRef,
    output_schema: DataSchemaRef,
    chain: Arc<ExpressionChain>,
    // whether to perform alias action in executor
    alias_project: bool,
}

impl ExpressionExecutor {
    pub fn try_create(
        description: &str,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        exprs: Vec<Expression>,
        alias_project: bool,
    ) -> Result<Self> {
        let chain = ExpressionChain::try_create(input_schema.clone(), &exprs)?;

        Ok(Self {
            description: description.to_string(),
            _input_schema: input_schema,
            output_schema,
            chain: Arc::new(chain),
            alias_project,
        })
    }

    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    pub fn execute(&self, block: &DataBlock) -> Result<DataBlock> {
        tracing::debug!(
            "({:#}) execute, actions: {:?}",
            self.description,
            self.chain.actions
        );

        let mut column_map: HashMap<&str, ColumnWithField> = HashMap::new();

        let mut alias_map: HashMap<&str, &ColumnWithField> = HashMap::new();

        // supported a + 1 as b, a + 1 as c
        // supported a + 1 as a, a as b
        // !currently not supported a+1 as c, b+1 as c
        let mut alias_action_map: HashMap<&str, Vec<&str>> = HashMap::new();

        for f in block.schema().fields().iter() {
            let column =
                ColumnWithField::new(block.try_column_by_name(f.name())?.clone(), f.clone());
            column_map.insert(f.name(), column);
        }

        let rows = block.num_rows();
        for action in self.chain.actions.iter() {
            if let ExpressionAction::Alias(alias) = action {
                if let Some(v) = alias_action_map.get_mut(alias.arg_name.as_str()) {
                    v.push(alias.name.as_str());
                } else {
                    alias_action_map.insert(alias.arg_name.as_str(), vec![alias.name.as_str()]);
                }
            }

            if column_map.contains_key(action.column_name()) {
                continue;
            }

            match action {
                ExpressionAction::Input(input) => {
                    let column = block.try_column_by_name(&input.name)?.clone();
                    let column = ColumnWithField::new(
                        column,
                        block.schema().field_with_name(&input.name)?.clone(),
                    );
                    column_map.insert(input.name.as_str(), column);
                }
                ExpressionAction::Function(f) => {
                    let column_with_field = self.execute_function(&mut column_map, f, rows)?;
                    column_map.insert(f.name.as_str(), column_with_field);
                }
                ExpressionAction::Constant(constant) => {
                    let column = constant
                        .data_type
                        .create_constant_column(&constant.value, rows)?;

                    let column = ColumnWithField::new(
                        column,
                        DataField::new(constant.name.as_str(), constant.data_type.clone()),
                    );

                    column_map.insert(constant.name.as_str(), column);
                }
                _ => {}
            }
        }

        if self.alias_project {
            for (k, v) in alias_action_map.iter() {
                let column = column_map.get(k).ok_or_else(|| {
                    ErrorCode::LogicalError("Arguments must be prepared before alias transform")
                })?;

                for name in v.iter() {
                    match alias_map.insert(name, column) {
                        Some(_) => Err(ErrorCode::UnImplement(format!(
                            "Duplicate alias name :{}",
                            name
                        ))),
                        _ => Ok(()),
                    }?;
                }
            }
        }

        let mut project_columns = Vec::with_capacity(self.output_schema.fields().len());
        for f in self.output_schema.fields() {
            let column = match alias_map.get(f.name().as_str()) {
                Some(data_column) => data_column,
                None => column_map.get(f.name().as_str()).ok_or_else(|| {
                    ErrorCode::LogicalError(format!(
                        "Projection column: {} not exists in {:?}, there are bugs!",
                        f.name(),
                        column_map.keys()
                    ))
                })?,
            };
            project_columns.push(column.column().clone());
        }
        // projection to remove unused columns
        Ok(DataBlock::create(
            self.output_schema.clone(),
            project_columns,
        ))
    }

    #[inline]
    fn execute_function(
        &self,
        column_map: &mut HashMap<&str, ColumnWithField>,
        f: &ActionFunction,
        rows: usize,
    ) -> Result<ColumnWithField> {
        // check if it's cached
        let mut arg_columns = Vec::with_capacity(f.arg_names.len());

        for arg in f.arg_names.iter() {
            let column = column_map.get(arg.as_str()).cloned().ok_or_else(|| {
                ErrorCode::LogicalError("Arguments must be prepared before function transform")
            })?;
            arg_columns.push(column);
        }

        let column = f.func.eval(&arg_columns, rows)?;
        Ok(ColumnWithField::new(
            column,
            DataField::new(&f.name, f.return_type.clone()),
        ))
    }
}
