// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_functions::{IFunction, AliasFunction, ColumnFunction, LiteralFunction, FunctionFactory, CastFunction};
use common_datavalues::{DataType, DataSchemaRef};
use common_exception::{Result, ErrorCodes};

use crate::ExpressionAction;
use common_aggregate_functions::{IAggreagteFunction, AggregateFunctionFactory};

// ActionType is some common types for action transformation
pub enum ActionType {
    INPUT,
    COLUMN,
    ALIAS,
    FUNCTION,
}

#[derive(Default, Clone, Debug)]
pub struct ActionNode {
    pub action_type: ActionType,
    pub name: String,

    // for scalar function
    pub func: Option<Box<dyn IFunction>>,
    // for aggregate function
    pub aggregate_func: Option<Box<dyn IAggreagteFunction>>,
    pub arg_names: Vec<String>,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
}

pub struct ExpressionChain {
    pub schema: DataSchemaRef,
    pub nodes: Vec<ActionNode>,
}

impl ExpressionChain {
    pub fn new(schema: DataSchemaRef) -> Self {
        Self {
            schema,
            nodes: vec![],
        }
    }

    pub fn add_expr(&mut self, expr: &ExpressionAction) -> Result<()> {
        match expr {
            ExpressionAction::Alias(name, sub_expr) => {
                self.add_expr(sub_expr)?;
                let func = AliasFunction::try_create(name.clone())?;
                let arg_types = vec![expr.to_data_type(&self.schema)?];
                let return_type = func.return_type(&arg_types)?;
                self.chain.push(ActionNode {
                    name: expr.column_name(),
                    func: Some(func),
                    arg_names: vec![sub_expr.column_name()],
                    arg_types,
                    return_type,
                    ..Default::default()
                });
            }
            ExpressionAction::Column(c) => {
                let func = ColumnFunction::try_create(c)?;
                let arg_type = self.schema.field_with_name(c).map_err(ErrorCodes::from_arrow)?.data_type();
                self.chain.push(ActionNode {
                    name: self.column_name(),
                    func: Some(func) ,
                    arg_names: vec![c.to_string()],
                    arg_types: vec![arg_type.clone()],
                    return_type: arg_type.clone()?,
                    ..Default::default()
                });
            }
            ExpressionAction::Literal(l) => {
                self.chain.push(ActionNode {
                    name: self.column_name(),
                    func: Some( LiteralFunction::try_create(l.clone())?) ,
                    return_type: l.data_type(),
                    ..Default::default()
                });
            }
            ExpressionAction::ScalarFunction { op, args } => {
                args.iter().for_each(|expr| self.add_expr(expr)?);

                let func = FunctionFactory::get(op)?;
                let arg_types = args.iter().map(|action| action.to_data_type(&self.schema)).collect::<Result<Vec<DataType>>>()?;
                let return_type = func.return_type(&arg_types)?;
                self.chain.push(ActionNode {
                    name: self.column_name(),
                    func: Some(func),
                    arg_names: args.iter().map(|action| action.column_name()).collect(),
                    arg_types,
                    return_type,
                    ..Default::default()
                });
            }
            ExpressionAction::AggregateFunction { op, args } => {
                args.iter().for_each(|expr| self.add_expr(expr)?);

                let func = AggregateFunctionFactory::get(op)?;
                let arg_types = args.iter().map(|action| action.to_data_type(&self.schema)).collect::<Result<Vec<DataType>>>()?;
                let return_type = func.return_type(&arg_types)?;
                self.chain.push(ActionNode {
                    name: self.column_name(),
                    aggregate_func: Some(func),
                    arg_names: args.iter().map(|action| action.column_name()).collect(),
                    arg_types,
                    return_type,
                    ..Default::default()
                });
            }
            ExpressionAction::Sort { expr, asc, nulls_first } => {
                self.add_expr(expr);
            }

            ExpressionAction::Wildcard => {}
            ExpressionAction::Cast { expr: sub_expr, data_type } => {
                self.add_expr(sub_expr)?;
                let func = CastFunction::create(data_type.clone());
                self.chain.push(ActionNode {
                    name: self.column_name(),
                    func: Some(func) ,
                    arg_names: vec![sub_expr.column_name().into()],
                    arg_types: vec![sub_expr.to_data_type(&self.schema)?],
                    return_type: data_type.clone(),
                    ..Default::default()
                });
            }
        }
        Ok(())
    }
}
