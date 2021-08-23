use common_functions::aggregates::AggregateFunctionRef;
use std::sync::Arc;
use common_planners::Expression;
use common_exception::Result;
use common_datavalues::DataSchemaRef;

pub struct AggregatorParams {
    pub aggregate_functions: Vec<AggregateFunctionRef>,
    pub aggregate_functions_column_name: Vec<String>,
    pub aggregate_functions_arguments_name: Vec<Vec<String>>,
}

pub type AggregatorParamsRef = Arc<AggregatorParams>;

impl AggregatorParams {
    pub fn try_create(schema: DataSchemaRef, exprs: &[Expression]) -> Result<AggregatorParamsRef> {
        let mut aggregate_functions = Vec::with_capacity(exprs.len());
        let mut aggregate_functions_column_name = Vec::with_capacity(exprs.len());
        let mut aggregate_functions_arguments_name = Vec::with_capacity(exprs.len());

        for expr in exprs.iter() {
            aggregate_functions.push(expr.to_aggregate_function(&schema)?);
            aggregate_functions_column_name.push(expr.column_name());
            aggregate_functions_arguments_name.push(expr.to_aggregate_function_names()?);
        }

        Ok(Arc::new(AggregatorParams {
            aggregate_functions,
            aggregate_functions_column_name,
            aggregate_functions_arguments_name,
        }))
    }
}

