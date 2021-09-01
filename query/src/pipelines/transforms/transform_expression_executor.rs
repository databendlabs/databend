// Copyright 2020 Datafuse Labs.
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
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
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
    input_schema: DataSchemaRef,
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
            input_schema,
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

        let mut column_map: HashMap<String, DataColumnWithField> = HashMap::new();

        // a + 1 as b, a + 1 as c
        let mut alias_map: HashMap<String, Vec<String>> = HashMap::new();

        for f in block.schema().fields().iter() {
            let column =
                DataColumnWithField::new(block.try_column_by_name(f.name())?.clone(), f.clone());
            column_map.insert(f.name().clone(), column);
        }

        let rows = block.num_rows();

        for action in self.chain.actions.iter() {
            if let ExpressionAction::Alias(alias) = action {
                if let Some(v) = alias_map.get_mut(&alias.arg_name) {
                    v.push(alias.name.clone());
                } else {
                    alias_map.insert(alias.arg_name.clone(), vec![alias.name.clone()]);
                }
            }

            if column_map.contains_key(action.column_name()) {
                continue;
            }

            match action {
                ExpressionAction::Input(input) => {
                    let column = block.try_column_by_name(&input.name)?.clone();
                    let column = DataColumnWithField::new(
                        column,
                        block.schema().field_with_name(&input.name)?.clone(),
                    );
                    column_map.insert(input.name.clone(), column);
                }
                ExpressionAction::Function(f) => {
                    // check if it's cached
                    let mut arg_columns = Vec::with_capacity(f.arg_names.len());

                    for arg in f.arg_names.iter() {
                        let column = column_map.get(arg).cloned().ok_or_else(|| {
                            ErrorCode::LogicalError(
                                "Arguments must be prepared before function transform",
                            )
                        })?;
                        arg_columns.push(column);
                    }

                    let func = f.to_function()?;
                    let column = func.eval(&arg_columns, rows)?;

                    let column = DataColumnWithField::new(
                        column,
                        DataField::new(&f.name, f.return_type.clone(), f.is_nullable),
                    );

                    column_map.insert(f.name.clone(), column);
                }
                ExpressionAction::Constant(constant) => {
                    let column = DataColumn::Constant(constant.value.clone(), rows);

                    let column = DataColumnWithField::new(
                        column,
                        DataField::new(
                            constant.name.as_str(),
                            constant.data_type.clone(),
                            constant.value.is_null(),
                        ),
                    );

                    column_map.insert(constant.name.clone(), column);
                }
                _ => {}
            }
        }

        if self.alias_project {
            for (k, v) in alias_map.iter() {
                let column = column_map.get(k).cloned().ok_or_else(|| {
                    ErrorCode::LogicalError("Arguments must be prepared before alias transform")
                })?;

                for name in v.iter() {
                    column_map.insert(name.clone(), column.clone());
                }
            }
        }

        let mut project_columns = Vec::with_capacity(self.output_schema.fields().len());
        for f in self.output_schema.fields() {
            let column = column_map.get(f.name()).ok_or_else(|| {
                ErrorCode::LogicalError(format!(
                    "Projection column: {} not exists in {:?}, there are bugs!",
                    f.name(),
                    column_map.keys()
                ))
            })?;
            project_columns.push(column.column().clone());
        }
        // projection to remove unused columns
        Ok(DataBlock::create(
            self.output_schema.clone(),
            project_columns,
        ))
    }
}
