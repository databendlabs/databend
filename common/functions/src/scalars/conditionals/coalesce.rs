use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_exception::ErrorCode;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

use super::IfFunction;
use super::IsNotNullFunction;

#[derive(Clone)]
pub struct CoalesceFunction {
    display_name: String,
    result_type: DataTypeImpl,
    if_fn: Result<Box<dyn Function, Global>, ErrorCode>,
    is_not_null_fn: Result<Box<dyn Function, Global>, ErrorCode>,

}

impl CoalesceFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        let mut first_not_null_opt: Option<DataTypeImpl> = None;
        let mut if_fn_args = vec![&DataTypeImpl::Boolean(bool)];
        // Check all the arguments have the same data type
        for arg_index in 0..args.len() {
            let data_type = remove_nullable(args[arg_index]);
            if !data_type.is_null() {
                if_fn_args.push(&data_type);
                match &first_not_null_opt {
                    None => first_not_null_opt = Some(data_type),
                    Some(first_not_null) => {
                            if first_not_null.data_type_id() != data_type.data_type_id() {
                                return Err(ErrorCode::IllegalDataType(
                                    "All the arguments for function coalesce must have the same data type",
                                ))   
                            }
                    },
                }
            }
            
        }
        
        let result_type = match first_not_null_opt  {
            Some(first_not_null) => NullableType::new_impl(first_not_null.clone()),
            None => NullableType::new_impl(NullType::new_impl()),
        };
        let if_fn = IfFunction::try_create("if", &if_fn_args);
        let is_not_null_fn = IsNotNullFunction::try_create_func("is_not_null", &if_fn_args);
        Ok(Box::new(CoalesceFunction{
            display_name: display_name.to_string(),
            result_type,
            if_fn,
            is_not_null_fn,
        }))
    }
    // The function coalesce returns the first non-NULL expression among its arguments, or NULL if all its arguments are NULL.
    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
               .deterministic()
               .variadic_arguments(2, 8)
        )
    }
    

    
}

impl Function for CoalesceFunction {
    fn name(&self) -> &str {
        "CoalesceFunction"
    }

    fn return_type(&self) -> DataTypeImpl {
       self.result_type.clone()   
    }

    fn eval(
            &self,
            _func_ctx: FunctionContext,
            columns: &ColumnsWithField,
            input_rows: usize,
        ) -> Result<ColumnRef> {
        
        if self.result_type.is_null() {
           Ok(NullColumn::new(input_rows).arc())
        }else {
           
           Result::Err(ErrorCode::UnImplement("the function coalesce is not complete"))
        }
    }

}

impl std::fmt::Display for CoalesceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "coalesce")
    }
}